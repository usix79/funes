using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Indexes;
using Funes.Sets;
using Microsoft.Extensions.Logging;

namespace Funes {
    public static class LogicEngine<TModel,TMsg,TSideEffect> {
        
        private static readonly Utils.ObjectPool<LogicState<TMsg, TSideEffect>> StatesPool =
            new(() => new LogicState<TMsg, TSideEffect>(), 12);
        
        public static async Task<Result<LogicResult<TSideEffect>>> Run(LogicEngineEnv<TModel,TMsg,TSideEffect> env,
            IDataSource ds, Entity fact, IConstants constants, CancellationToken ct) {
            
            var lgResult = new LogicResult<TSideEffect>();
            var lgState = StatesPool.Rent();

            var iteration = 0;
            try {
                var (model, cmd) = env.Logic.Begin(fact, constants);
                await env.Tracer.BeginResult(fact, model, cmd);
                ProcessCommand(cmd);

                while (lgState.InProcessing) {
                    ct.ThrowIfCancellationRequested();

                    // TODO: consider continue work after exception in update()
                    while (lgState.PendingMessages.TryDequeue(out var msg)) {
                        if (iteration++ > env.IterationsLimit) ThrowIterationsLimitException(env.IterationsLimit);
                        (model, cmd) = env.Logic.Update(model, msg);
                        await env.Tracer.UpdateResult(msg, model, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (lgState.ShouldWait) {
                        await lgState.WhenAnyPendingTasks(ct);
                        ProcessPendingCommands();
                    }
                }

                cmd = env.Logic.End(model);
                await env.Tracer.EndResult(cmd);
                ProcessCommand(cmd);
                return new Result<LogicResult<TSideEffect>>(lgResult);
            }
            catch (TaskCanceledException) {
                throw;
            }
            catch (Exception e) {
                return Result<LogicResult<TSideEffect>>.Exception(e);
            }
            finally {
                lgState.Reset();
                StatesPool.Return(lgState);
            }
            
            void ProcessCommand(Cmd<TMsg, TSideEffect> aCmd) {
                switch (aCmd) {
                    case Cmd<TMsg, TSideEffect>.NoneCmd:
                        break;
                    case Cmd<TMsg, TSideEffect>.MsgCmd x:
                        lgState.PendingMessages.Enqueue(x.Msg);
                        break;
                    case Cmd<TMsg, TSideEffect>.BatchCmd x:
                        foreach (var item in x.Items) 
                            ProcessCommand(item);
                        break;
                    case Cmd<TMsg, TSideEffect>.BatchOutputCmd x:
                        foreach (var item in x.Items) 
                            ProcessCommand(item);
                        break;
                    case Cmd<TMsg, TSideEffect>.UploadCmd x:
                        lgResult.Entities[x.Entity.Id] = x.Entity;
                        break;
                    case Cmd<TMsg, TSideEffect>.SetCmd x:
                        if (!lgResult.SetRecords.TryGetValue(x.SetName, out var setRecord)) {
                            setRecord = new SetRecord();
                            lgResult.SetRecords[x.SetName] = setRecord;
                        }
                        setRecord.Add(new SetOp(x.Op, x.Tag));
                        break;
                    case Cmd<TMsg, TSideEffect>.IndexCmd x:
                        if (!lgResult.IndexRecords.TryGetValue(x.IndexName, out var indexRecord)) {
                            indexRecord = new IndexRecord();
                            lgResult.IndexRecords[x.IndexName] = indexRecord;
                        }
                        indexRecord.Add(new IndexOp(x.Op, x.Key, x.Value));
                        break;
                    case Cmd<TMsg, TSideEffect>.SideEffectCmd x:
                        lgResult.SideEffects.Add(x.SideEffect);
                        break;
                    case Cmd<TMsg, TSideEffect>.ConstantCmd x:
                        lgResult.Constants.Add(new KeyValuePair<string, string>(x.Name, x.Value));
                        break;
                    case Cmd<TMsg, TSideEffect>.RetrieveCmd x:
                        if (!TryCompleteRetrieve(x)) {
                            lgState.PendingCommands.AddLast(x);
                            StartRetrievingTask(x.EntityId);
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.RetrieveManyCmd x:
                        if (!TryCompleteRetrieveMany(x)) {
                            lgState.PendingCommands.AddLast(x);
                            foreach (var memId in x.EntityIds)
                                StartRetrievingTask(memId);
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.RetrieveSetCmd x:
                        if (!TryCompleteRetrieveSet(x)) {
                            lgState.PendingCommands.AddLast(x);
                            StartRetrievingSetTask(x.SetName);
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.SelectCmd x:
                        StartSelectionTask(x);
                        break;
                    case Cmd<TMsg, TSideEffect>.GetIndexValueCmd x:
                        if (!TryCompleteIndexValue(x)) {
                            lgState.PendingCommands.AddLast(x);
                            StartRetrievingTask(IndexesModule.GetKeyId(x.IndexName, x.Key));
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.LogCmd x:
                        env.Logger.Log(x.Level, x.Message, x.Args);
                        break;
                }
            }

            bool TryCompleteRetrieve(Cmd<TMsg, TSideEffect>.RetrieveCmd aCmd) {
                if (!ds.TryGetEntry(aCmd.EntityId, aCmd.AsPremise, out var entry)) return false;

                try {
                    lgState.PendingMessages.Enqueue(aCmd.Action(entry));
                }
                catch (Exception x) {
                    env.Logger.LogError(x, "Failed retrieve action for {EntityId}", aCmd.EntityId.Id);
                }
                return true;
            }

            bool TryCompleteRetrieveMany(Cmd<TMsg, TSideEffect>.RetrieveManyCmd aCmd) {
                var arr = ArrayPool<EntityEntry>.Shared.Rent(aCmd.EntityIds.Length);
                var entries = new ArraySegment<EntityEntry>(arr, 0, aCmd.EntityIds.Length);

                try {
                    for (var i = 0; i < aCmd.EntityIds.Length; i++) {
                        if (!ds.TryGetEntry(aCmd.EntityIds[i], aCmd.AsPremise, out var entry))
                            return false;
                        entries[i] = entry;
                    }
                    try {
                        lgState.PendingMessages.Enqueue(aCmd.Action(entries));
                    }
                    catch (Exception x) {
                        env.Logger.FunesException("RetrieveMany Action", 
                            aCmd.EntityIds.Length.ToString(), IncrementId.None, x);
                    }
                    return true;
                }
                finally {
                    ArrayPool<EntityEntry>.Shared.Return(arr);
                }
            }
            
            void StartRetrievingTask(EntityId entityId) {
                if (!lgState.StartedRetrieving.Contains(entityId)) {
                    lgState.PendingTasks.Add(ds.Retrieve(entityId, ct).AsTask());
                }
            }
            
            bool TryCompleteRetrieveSet(Cmd<TMsg, TSideEffect>.RetrieveSetCmd aCmd) {
                var setResult = ds.GetSet(aCmd.SetName);
                if (setResult.Error == Error.NotFound) return false;
                
                try {
                    var msg = setResult.IsOk ? aCmd.OnSuccess(setResult.Value) : aCmd.OnError(setResult.Error);
                    lgState.PendingMessages.Enqueue(msg);
                }
                catch (Exception x) {
                    env.Logger.FunesException("LogicEngine", "Action", IncrementId.None, x);
                }
                return true;
            }

            void StartRetrievingSetTask(string setName) {
                if (!lgState.StartedSetRetrieving.Contains(setName)) 
                    lgState.PendingTasks.Add(ds.RetrieveSetSnapshot(setName, ct).AsTask());
            }

            void StartSelectionTask(Cmd<TMsg, TSideEffect>.SelectCmd aCmd) {
                var task = IndexesModule.Select(ds, ct,
                    aCmd.IndexName, aCmd.FromValue, aCmd.ToValue, aCmd.AfterKey, aCmd.MaxCount).AsTask();

                lgState.PendingTasks.Add(task);
                lgState.StartedSelections[aCmd.GetHashCode()] = task;
            }

            bool TryCompleteSelection(Cmd<TMsg, TSideEffect>.SelectCmd aCmd) {
                if (!lgState.StartedSelections.TryGetValue(aCmd.GetHashCode(), out var task)) return false;

                if (task.IsCompleted) {
                    lgState.StartedSelections.Remove(aCmd.GetHashCode());
                    if (task.IsCompletedSuccessfully && task.Result.IsOk) {
                        lgState.PendingMessages.Enqueue(aCmd.OnSuccess(task.Result.Value.Pairs, task.Result.Value.HasMore));
                    }
                    else {
                        var error = task.IsCompletedSuccessfully 
                            ? task.Result.Error 
                            : task.Exception != null ? new Error.ExceptionError(task.Exception) : new Error.NoError();
                        lgState.PendingMessages.Enqueue(aCmd.OnError(error));
                    }
                }

                return true;
            }
            
            bool TryCompleteIndexValue(Cmd<TMsg, TSideEffect>.GetIndexValueCmd aCmd) {
                var keyId = IndexesModule.GetKeyId(aCmd.IndexName, aCmd.Key);
                if (!ds.TryGetEntity(keyId, out var stampResult)) return false;

                try {
                    var msg = stampResult.IsOk
                            ? aCmd.OnSuccess(IndexKeyHelpers.GetValue(stampResult.Value.Data))
                            : stampResult.Error == Error.NotFound
                                ? aCmd.OnSuccess("")
                                : aCmd.OnNotAvailable();
                    lgState.PendingMessages.Enqueue(msg);
                }
                catch (Exception x) {
                    env.Logger.LogError(x, "Failed index value action for {IndexName} {Key}", aCmd.IndexName, aCmd.Key);
                }
                return true;
            }

            void ProcessPendingCommands() {
                var node = lgState.PendingCommands.First;
                while (node != null) {
                    var nextNode = node.Next;
                    if (node.Value switch {
                        Cmd<TMsg, TSideEffect>.RetrieveCmd x => TryCompleteRetrieve(x),
                        Cmd<TMsg, TSideEffect>.RetrieveManyCmd x => TryCompleteRetrieveMany(x),
                        Cmd<TMsg, TSideEffect>.RetrieveSetCmd x => TryCompleteRetrieveSet(x),
                        Cmd<TMsg, TSideEffect>.SelectCmd x => TryCompleteSelection(x),
                        _ => true}) {
                        lgState.PendingCommands.Remove(node);
                    }
                    node = nextNode;
                }
            }
        }
        
        private static void ThrowIterationsLimitException(int limit) {
            var txt = $"Total number of iterations is over {limit}. Check logic for infinite loops.";
            throw new Exception(txt);                
        }
    }
}