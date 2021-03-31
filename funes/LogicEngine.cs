using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class LogicEngine<TModel,TMsg,TSideEffect> {

        private readonly int _iterationsLimit;
        private readonly ILogger _logger;
        private readonly ITracer<TModel,TMsg,TSideEffect> _tracer;
        private readonly ISerializer _serializer;
        private readonly IDataSource _ds;
        private readonly ILogic<TModel,TMsg,TSideEffect> _logic;

        public LogicEngine(ILogic<TModel,TMsg,TSideEffect> logic, 
                            ISerializer serializer,
                            IDataSource dr,
                            ILogger logger, 
                            ITracer<TModel,TMsg,TSideEffect> tracer, 
                            int iterationsLimit = 100500) {
            (_ds, _logic, _serializer, _logger, _tracer, _iterationsLimit) = 
                (dr, logic, serializer, logger, tracer, iterationsLimit);
        }
        
        public class LogicResult {
            public Dictionary<EntityId, (Entity,CognitionId,bool)> Inputs { get; } = new ();
            public Dictionary<EntityId, Entity> Outputs { get; } = new ();
            public Dictionary<EntityId, Entity> DerivedFacts { get; } = new ();
            public List<TSideEffect> SideEffects { get; } = new ();
            public NameValueCollection Constants { get; } = new ();
        }

        public async Task<Result<LogicResult>> Run(Entity fact, NameValueCollection constants, CancellationToken ct = default) {
            var entities = new Dictionary<EntityId, EntityEntry>{[fact.Id] = EntityEntry.Ok(fact)};
            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd<TMsg, TSideEffect>>();
            var retrievingTasks = new Dictionary<EntityId, ValueTask<Result<EntityEntry>>>();
            var output = new LogicResult();
            var iteration = 0;

            try {
                var (model, cmd) = _logic.Begin(fact, constants);
                await _tracer.BeginResult(fact, model, cmd);
                ProcessCommand(cmd);

                while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                    ct.ThrowIfCancellationRequested();
                    
                    // TODO: consider simultaneous execution 
                    // TODO: consider continue work after exception in update()
                    while (pendingMessages.TryDequeue(out var msg)) {
                        if (iteration++ > _iterationsLimit) ThrowIterationsLimitException();
                        (model, cmd) = _logic.Update(model, msg);
                        await _tracer.UpdateResult(model, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                        await Task.WhenAny(retrievingTasks.Values.Select(x => x.AsTask()));
                        ProcessPendingCommands();
                    }
                }

                cmd = _logic.End(model);
                await _tracer.EndResult(cmd);
                ProcessCommand(cmd);
                return new Result<LogicResult>(output);
            }
            catch (Exception e) {
                return Result<LogicResult>.Exception(e);
            }
            
            void ProcessCommand(Cmd<TMsg, TSideEffect> aCmd) {
                switch (aCmd) {
                    case Cmd<TMsg, TSideEffect>.NoneCmd:
                        break;
                    case Cmd<TMsg, TSideEffect>.MsgCmd x:
                        pendingMessages.Enqueue(x.Msg);
                        break;
                    case Cmd<TMsg, TSideEffect>.BatchCmd x:
                        foreach (var item in x.Items) ProcessCommand(item);
                        break;
                    case Cmd<TMsg, TSideEffect>.BatchOutputCmd x:
                        foreach (var item in x.Items) ProcessCommand(item);
                        break;
                    case Cmd<TMsg, TSideEffect>.ConclusionCmd x:
                        output.Outputs[x.Entity.Id] = x.Entity;
                        entities[x.Entity.Id] = EntityEntry.Ok(x.Entity);
                        break;
                    case Cmd<TMsg, TSideEffect>.DerivedFactCmd x:
                        output.DerivedFacts[x.Entity.Id] = x.Entity;
                        entities[x.Entity.Id] = EntityEntry.Ok(x.Entity);
                        break;
                    case Cmd<TMsg, TSideEffect>.SideEffectCmd x:
                        output.SideEffects.Add(x.SideEffect);
                        break;
                    case Cmd<TMsg, TSideEffect>.ConstantCmd x:
                        output.Constants.Add(x.Name, x.Value);
                        break;
                    case Cmd<TMsg, TSideEffect>.RetrieveCmd x:
                        if (!TryCompleteRetrieve(x)) {
                            pendingCommands.AddLast(x);
                            StartRetrievingTask(x.EntityId);
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.RetrieveManyCmd x:
                        if (!TryCompleteRetrieveMany(x)) {
                            pendingCommands.AddLast(x);
                            foreach (var memId in x.EntityIds)
                                StartRetrievingTask(memId);
                        }
                        break;
                    case Cmd<TMsg, TSideEffect>.LogCmd x:
                        _logger.Log(x.Level, x.Message, x.Args);
                        break;
                }
            }

            bool TryCompleteRetrieve(Cmd<TMsg, TSideEffect>.RetrieveCmd aCmd) {
                if (!entities!.TryGetValue(aCmd.EntityId, out var entry)) return false;

                AddInput(entry, aCmd.AsPremise);

                try {
                    pendingMessages!.Enqueue(aCmd.Action(entry));
                }
                catch (Exception x) {
                    _logger.LogError(x, "Failed retrieve action for {EntityId}", aCmd.EntityId);
                }
                return true;
            }

            bool TryCompleteRetrieveMany(Cmd<TMsg, TSideEffect>.RetrieveManyCmd aCmd) {
                if (aCmd.EntityIds.Any(x => !entities!.ContainsKey(x))) return false;

                var entries = aCmd.EntityIds.Select(memId => entities![memId]).ToArray();
                
                foreach (var entry in entries) AddInput(entry, aCmd.AsPremise);

                try {
                    pendingMessages!.Enqueue(aCmd.Action(entries));
                }
                catch (Exception x) {
                    _logger.LogError(x, "Failed retrieve many action for {EntityIds}", aCmd.EntityIds);
                }
                return true;
            }

            void AddInput(EntityEntry entry, bool asPremise) {
                if (entry.IsOk) {
                    if (output!.Inputs.TryGetValue(entry.Entity.Id, out var tuple)) {
                        output.Inputs[entry.Entity.Id] = (tuple.Item1, tuple.Item2,  tuple.Item3 || asPremise);
                    }
                    else {
                        output.Inputs[entry.Entity.Id] = (entry.Entity, entry.Cid, asPremise);
                    }
                }
            }

            void StartRetrievingTask(EntityId entityId) {
                if (!retrievingTasks.ContainsKey(entityId)) 
                    retrievingTasks[entityId] = _ds.Retrieve(entityId, _serializer, ct);
            }

            void ProcessRetrievingTasks() {
                if (retrievingTasks.Values.Any(x => x.IsCompleted)) {
                    foreach (var eid in retrievingTasks.Keys.ToArray()) {
                        var task = retrievingTasks[eid];
                        if (task.IsCompleted) {
                            retrievingTasks.Remove(eid);
                            var result = task.IsCompletedSuccessfully
                                ? task.Result : Result<EntityEntry>.Exception(task.AsTask().Exception!);
                            RegisterEntity(eid, result);
                        }
                    }
                }
            }

            void ProcessPendingCommands() {
                ProcessRetrievingTasks();
                
                var node = pendingCommands.First;
                while (node != null) {
                    if (node.Value switch {
                        Cmd<TMsg, TSideEffect>.RetrieveCmd x => TryCompleteRetrieve(x),
                        Cmd<TMsg, TSideEffect>.RetrieveManyCmd x => TryCompleteRetrieveMany(x),
                        _ => true}) {
                        pendingCommands.Remove(node);
                    }
                    node = node.Next;
                }
            }
                
            void RegisterEntity(EntityId eid, Result<EntityEntry> result) {
                if (!entities!.ContainsKey(eid)) {
                    entities[eid] = result.IsOk ? result.Value : EntityEntry.NotAvailable(eid);
                }
            }
        }
        
        private void ThrowIterationsLimitException() {
            var txt = $"Total number of iterations is over {_iterationsLimit}. Check logic for infinite loops.";
            throw new Exception(txt);                
        }
    }
}