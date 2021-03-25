using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public delegate ValueTask<Result<EntityStamp>> Retrieve(EntityId id, CancellationToken ct);

    public class LogicEngine<TState,TMsg,TSideEffect> {

        private readonly int _iterationsLimit;
        private readonly ITracer<TState,TMsg,TSideEffect>? _tracer;
        private readonly IDataRetriever _dr;
        private readonly ILogic<TState,TMsg,TSideEffect> _logic;

        public LogicEngine(ILogic<TState,TMsg,TSideEffect> logic, IDataRetriever dr,
                            ITracer<TState,TMsg,TSideEffect>? tracer = null, int iterationsLimit = 100500) {
            _dr = dr;
            _logic = logic;
            _tracer = tracer;
            _iterationsLimit = iterationsLimit;
        }
        
        public class Output {
            public HashSet<EntityStampKey> Premises { get; } = new ();
            public Dictionary<EntityId, Entity> Conclusions { get; } = new ();
            public Dictionary<EntityId, Entity> DerivedFacts { get; } = new ();
            public List<TSideEffect> SideEffects { get; } = new ();
        }
        
        
        // TODO: consider using of arbitrary constants for making logic reproducible even it needs RNG or new GUIDS
        //       receive the constants from end()
        //       store the constants in the reflection
        //       pass the constants to the begin() if reflection has been already happened
        public async Task<Result<Output>> Run(Entity fact, CancellationToken ct = default) {
            var mems = new Dictionary<EntityId, EntityEntry>{[fact.Id] = EntityEntry.Ok(fact)};
            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd<TMsg, TSideEffect>>();
            var retrievingTasks = new Dictionary<EntityId, ValueTask<Result<EntityStamp>>>();
            var output = new Output();
            var iteration = 0;

            try {
                var (state, cmd) = _logic.Begin(fact);
                if (_tracer != null) await _tracer.BeginResult(fact, state, cmd);
                ProcessCommand(cmd);

                while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                    ct.ThrowIfCancellationRequested();
                    
                    // TODO: consider simultaneous execution 
                    // TODO: consider continue work after exception in update()
                    while (pendingMessages.TryDequeue(out var msg)) {
                        if (iteration++ > _iterationsLimit) ThrowIterationsLimitException();
                        (state, cmd) = _logic.Update(state, msg);
                        if (_tracer != null) await _tracer.UpdateResult(state, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                        await Task.WhenAny(retrievingTasks.Values.Select(x => x.AsTask()));
                        ProcessPendingCommands();
                    }
                }

                cmd = _logic.End(state);
                if (_tracer != null) await _tracer.EndResult(cmd);
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
                        mems[x.Entity.Id] = EntityEntry.Ok(x.Entity);
                        break;
                    case Cmd<TMsg, TSideEffect>.DerivedFactCmd x:
                        output.DerivedFacts[x.Entity.Id] = x.Entity;
                        mems[x.Entity.Id] = EntityEntry.Ok(x.Entity);
                        break;
                    case Cmd<TMsg, TSideEffect>.SideEffectCmd x:
                        output.SideEffects.Add(x.SideEffect);
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
                            foreach(var memId in x.MemIds)
                                StartRetrievingTask(memId);
                        }
                        break;
                }
            }

            bool TryCompleteRetrieve(Cmd<TMsg, TSideEffect>.RetrieveCmd aCmd) {
                if (!mems!.TryGetValue(aCmd.EntityId, out var holder)) return false;
                
                pendingMessages!.Enqueue(aCmd.Action(holder));
                return true;
            }

            bool TryCompleteRetrieveMany(Cmd<TMsg, TSideEffect>.RetrieveManyCmd aCmd) {
                if (aCmd.MemIds.Any(x => !mems!.ContainsKey(x))) return false;

                var holders = aCmd.MemIds.Select(memId => mems![memId]).ToArray();
                pendingMessages!.Enqueue(aCmd.Action(holders));
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
                                RegisterPremise(memId, task.Result);
                            }
                            catch (Exception x) {
                                RegisterPremise(memId, Result<EntityStamp>.Exception(x));
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
                
            void RegisterPremise(EntityId memId, Result<EntityStamp> result) {
                if (!mems!.ContainsKey(memId)) {
                    mems[memId] = result.IsOk
                        ? EntityEntry.Ok(result.Value.Entity)
                        : result.Error == Error.NotFound
                            ? EntityEntry.NotExist
                            : EntityEntry.NotAvailable;
                    
                    if (result.IsOk) output!.Premises.Add(result.Value.Key);
                }
            }
        }
        
        private void ThrowIterationsLimitException() {
            var txt = $"Total number of iterations is over {_iterationsLimit}. Check logic for infinite loops.";
            throw new Exception(txt);                
        }
    }
}