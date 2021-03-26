using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class LogicEngine<TState,TMsg,TSideEffect> {

        private readonly int _iterationsLimit;
        private readonly ILogger _logger;
        private readonly ITracer<TState,TMsg,TSideEffect> _tracer;
        private readonly IDataRetriever _dr;
        private readonly ILogic<TState,TMsg,TSideEffect> _logic;

        public LogicEngine(ILogic<TState,TMsg,TSideEffect> logic, 
                            IDataRetriever dr,
                            ILogger logger, 
                            ITracer<TState,TMsg,TSideEffect> tracer, 
                            int iterationsLimit = 100500) {
            (_dr, _logic, _logger, _tracer, _iterationsLimit) = 
                (dr, logic, logger, tracer, iterationsLimit);
        }
        
        public class Output {
            public Dictionary<EntityId, (Entity,ReflectionId)> Premises { get; } = new ();
            public Dictionary<EntityId, Entity> Conclusions { get; } = new ();
            public Dictionary<EntityId, Entity> DerivedFacts { get; } = new ();
            public List<TSideEffect> SideEffects { get; } = new ();

            public NameValueCollection Constants { get; } = new ();
        }
        
        
        // TODO: consider using of arbitrary constants for making logic reproducible even it needs RNG or new GUIDS
        //       receive the constants from end()
        //       store the constants in the reflection
        //       pass the constants to the begin() if reflection has been already happened
        public async Task<Result<Output>> Run(Entity fact, NameValueCollection constants, CancellationToken ct = default) {
            var entities = new Dictionary<EntityId, EntityEntry>{[fact.Id] = EntityEntry.Ok(fact)};
            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd<TMsg, TSideEffect>>();
            var retrievingTasks = new Dictionary<EntityId, ValueTask<Result<EntityStamp>>>();
            var output = new Output();
            var iteration = 0;

            try {
                var (state, cmd) = _logic.Begin(fact, constants);
                await _tracer.BeginResult(fact, state, cmd);
                ProcessCommand(cmd);

                while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                    ct.ThrowIfCancellationRequested();
                    
                    // TODO: consider simultaneous execution 
                    // TODO: consider continue work after exception in update()
                    while (pendingMessages.TryDequeue(out var msg)) {
                        if (iteration++ > _iterationsLimit) ThrowIterationsLimitException();
                        (state, cmd) = _logic.Update(state, msg);
                        await _tracer.UpdateResult(state, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                        await Task.WhenAny(retrievingTasks.Values.Select(x => x.AsTask()));
                        ProcessPendingCommands();
                    }
                }

                cmd = _logic.End(state);
                await _tracer.EndResult(cmd);
                ProcessCommand(cmd);
                return new Result<Output>(output);
            }
            catch (Exception e) {
                return Result<Output>.Exception(e);
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
                        output.Conclusions[x.Entity.Id] = x.Entity;
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

                // move to premises if needed
                if (entry.IsOk && aCmd.AsPremise) output.Premises[entry.Entity.Id] = (entry.Entity, entry.Rid);

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

                // move to premises if needed
                if (aCmd.AsPremise) {
                    foreach (var entry in entries) {
                        if (entry.IsOk) output.Premises[entry.Entity.Id] = (entry.Entity, entry.Rid);
                    }
                }

                try {
                    pendingMessages!.Enqueue(aCmd.Action(entries));
                }
                catch (Exception x) {
                    _logger.LogError(x, "Failed retrieve many action for {EntityIds}", aCmd.EntityIds);
                }
                return true;
            }

            void StartRetrievingTask(EntityId memId) {
                if (!retrievingTasks.ContainsKey(memId)) retrievingTasks[memId] = _dr.Retrieve(memId, ct);
            }

            void ProcessRetrievingTasks() {
                if (retrievingTasks.Values.Any(x => x.IsCompleted)) {
                    foreach (var memId in retrievingTasks.Keys.ToArray()) {
                        var task = retrievingTasks[memId];
                        if (task.IsCompleted) {
                            retrievingTasks.Remove(memId);
                            try {
                                RegisterEntity(memId, task.Result);
                            }
                            catch (Exception x) {
                                RegisterEntity(memId, Result<EntityStamp>.Exception(x));
                            }
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
                
            void RegisterEntity(EntityId eid, Result<EntityStamp> result) {
                if (!entities!.ContainsKey(eid)) {
                    entities[eid] = result.IsOk
                        ? EntityEntry.Ok(result.Value.Entity, result.Value.Rid)
                        : result.Error == Error.NotFound
                            ? EntityEntry.NotExist
                            : EntityEntry.NotAvailable;
                }
            }
        }
        
        private void ThrowIterationsLimitException() {
            var txt = $"Total number of iterations is over {_iterationsLimit}. Check logic for infinite loops.";
            throw new Exception(txt);                
        }
    }
}