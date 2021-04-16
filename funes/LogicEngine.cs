using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Sets;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class LogicEngine<TModel,TMsg,TSideEffect> {

        private readonly int _iterationsLimit;
        private readonly ILogger _logger;
        private readonly ITracer<TModel,TMsg,TSideEffect> _tracer;
        private readonly ISerializer _serializer;
        private readonly ISerializer _sysSerializer = new SystemSerializer();
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
            public Dictionary<EntityId, Entity> Entities { get; } = new ();
            public Dictionary<string, SetRecord> SetRecords { get; } = new();
            public List<TSideEffect> SideEffects { get; } = new ();
            public List<KeyValuePair<string, string>> Constants { get; } = new ();
        }

        public async Task<Result<LogicResult>> Run(Entity fact, 
            IConstants constants, IIncrementArgsCollector argsCollector, CancellationToken ct) {
            var entities = new Dictionary<EntityId, EntityEntry>{[fact.Id] = EntityEntry.Ok(fact)};
            var sets = new Dictionary<string, SetSnapshot>();

            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd<TMsg, TSideEffect>>();
            var retrievingTasks = new Dictionary<EntityId, ValueTask<Result<EntityEntry>>>();
            var retrievingSetTasks = new Dictionary<string, Task<Result<SetSnapshot>>>();
            
            var output = new LogicResult();
            var iteration = 0;

            try {
                var (model, cmd) = _logic.Begin(fact, constants);
                await _tracer.BeginResult(fact, model, cmd);
                ProcessCommand(cmd);

                while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                    ct.ThrowIfCancellationRequested();
                    
                    // TODO: consider continue work after exception in update()
                    while (pendingMessages.TryDequeue(out var msg)) {
                        if (iteration++ > _iterationsLimit) ThrowIterationsLimitException();
                        (model, cmd) = _logic.Update(model, msg);
                        await _tracer.UpdateResult(msg, model, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                        var tasksArr = ArrayPool<Task>.Shared.Rent(
                            retrievingTasks.Count + retrievingSetTasks.Count);

                        try {
                            var idx = 0;
                            foreach (var task in retrievingTasks.Values) tasksArr[idx++] = task.AsTask();
                            foreach (var task in retrievingSetTasks.Values) tasksArr[idx++] = task;
                            while (idx < tasksArr.Length) tasksArr[idx++] = Task.CompletedTask;

                            await Task.WhenAny(tasksArr);
                        }
                        finally {
                            ArrayPool<Task>.Shared.Return(tasksArr);
                        }
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
                    case Cmd<TMsg, TSideEffect>.UploadCmd x:
                        output.Entities[x.Entity.Id] = x.Entity;
                        entities[x.Entity.Id] = EntityEntry.Ok(x.Entity);
                        break;
                    case Cmd<TMsg, TSideEffect>.SetCmd x:
                        if (!output.SetRecords.TryGetValue(x.SetName, out var setRecord)) {
                            setRecord = new SetRecord();
                            output.SetRecords[x.SetName] = setRecord;
                        }
                        setRecord.Add(new SetOp(x.Op, x.Tag));
                        break;
                    case Cmd<TMsg, TSideEffect>.SideEffectCmd x:
                        output.SideEffects.Add(x.SideEffect);
                        break;
                    case Cmd<TMsg, TSideEffect>.ConstantCmd x:
                        output.Constants.Add(new KeyValuePair<string, string>(x.Name, x.Value));
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
                    case Cmd<TMsg, TSideEffect>.RetrieveSetCmd x:
                        if (!TryCompleteRetrieveSet(x)) {
                            pendingCommands.AddLast(x);
                            StartRetrievingSetTask(x.SetName, argsCollector);
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
                    _logger.LogError(x, "Failed retrieve action for {EntityId}", aCmd.EntityId.Id);
                }
                return true;
            }

            bool TryCompleteRetrieveMany(Cmd<TMsg, TSideEffect>.RetrieveManyCmd aCmd) {
                if (aCmd.EntityIds.Any(x => !entities!.ContainsKey(x))) return false;

                var entries = aCmd.EntityIds.Select(eid => entities![eid]).ToArray();
                
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
                    argsCollector.RegisterEntity(entry.Key, asPremise);
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

            bool TryCompleteRetrieveSet(Cmd<TMsg, TSideEffect>.RetrieveSetCmd aCmd) {
                if (!sets!.TryGetValue(aCmd.SetName, out var snapshot)) return false;
                
                try {
                    pendingMessages!.Enqueue(aCmd.Action(snapshot));
                }
                catch (Exception x) {
                    _logger.FunesException("LogicEngine", "Action", IncrementId.None, x);
                }
                return true;
            }

            void StartRetrievingSetTask(string setName, IIncrementArgsCollector argsCollector) {
                if (!retrievingSetTasks!.ContainsKey(setName)) 
                    retrievingSetTasks[setName] = SetsHelpers.RetrieveSnapshot(_ds, _sysSerializer, setName, argsCollector, ct);
            }
            
            void ProcessRetrievingSetTasks() {
                if (retrievingSetTasks!.Values.Any(x => x.IsCompleted)) {
                    foreach (var setName in retrievingSetTasks.Keys.ToArray()) {
                        var task = retrievingSetTasks[setName];
                        if (task.IsCompleted) {
                            retrievingSetTasks.Remove(setName);
                            if (task.IsCompletedSuccessfully && task.Result.IsOk) {
                                sets![setName] = task.Result.Value;
                            }
                            else {
                                sets![setName] = SetSnapshot.Empty;
                            }
                        }
                    }
                }
            }
            
            void ProcessPendingCommands() {
                ProcessRetrievingTasks();
                ProcessRetrievingSetTasks();
                
                var node = pendingCommands.First;
                while (node != null) {
                    var nextNode = node.Next;
                    if (node.Value switch {
                        Cmd<TMsg, TSideEffect>.RetrieveCmd x => TryCompleteRetrieve(x),
                        Cmd<TMsg, TSideEffect>.RetrieveManyCmd x => TryCompleteRetrieveMany(x),
                        Cmd<TMsg, TSideEffect>.RetrieveSetCmd x => TryCompleteRetrieveSet(x),
                        _ => true}) {
                        pendingCommands.Remove(node);
                    }
                    node = nextNode;
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