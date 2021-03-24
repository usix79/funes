using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    
    public struct MemHolder {
        private enum Status { IsNotAvailable = 0, IsNotExist, IsOk }
        
        private Status _status;
        public Mem Mem { get; }
        public bool IsNotAvailable => _status == Status.IsNotAvailable;
        public bool IsNotExist => _status == Status.IsNotExist;
        public bool IsOk => _status == Status.IsOk;

        private MemHolder(Mem mem) => (Mem, _status) = (mem, Status.IsOk);
        public static MemHolder Ok(Mem mem) => new (mem);
        public static readonly MemHolder NotAvailable = new MemHolder {_status = Status.IsNotAvailable};
        public static readonly MemHolder NotExist = new MemHolder {_status = Status.IsNotExist};
    }

    public delegate ValueTask<Result<MemStamp>> Retrieve(MemId id, CancellationToken ct);

    public class LogicEngine<TState,TMsg,TSideEffect> {

        private readonly int _iterationsLimit;
        private readonly ITracer? _tracer;
        private readonly Retrieve _retrieve;
        private readonly ILogic _logic;

        public LogicEngine(Retrieve retrieve, ILogic logic, ITracer? tracer = null, int iterationsLimit = 100500) {
            _retrieve = retrieve;
            _logic = logic;
            _tracer = tracer;
            _iterationsLimit = iterationsLimit;
        }
        
        public interface ILogic {
            public (TState, Cmd) Begin(Mem mem);
            public (TState, Cmd) Update(TState state, TMsg msg);
            public Cmd.OutputCmd End(TState state);
        }
        
        // TODO: consider private constructors and static creators
        public abstract record Cmd {
            public record NoneCmd : Cmd;
            public record MsgCmd(TMsg Msg) : Cmd;
            public record RetrieveCmd(MemId MemId, Func<MemHolder, TMsg> Action) : Cmd;
            public record RetrieveManyCmd(IEnumerable<MemId> MemIds, Func<MemHolder[], TMsg> Action) : Cmd;
            public record BatchCmd(IEnumerable<Cmd> Items) : Cmd;
            public abstract record OutputCmd : Cmd;
            public record ConclusionCmd(Mem Mem) : OutputCmd;
            public record DerivedFactCmd(Mem Mem) : OutputCmd;
            public record SideEffectCmd(TSideEffect SideEffect) : OutputCmd;
            public record BatchOutputCmd(IEnumerable<OutputCmd> Items) : OutputCmd;

            public static readonly Cmd None = new NoneCmd();
        }
        
        public interface ITracer {
            ValueTask Begin(TState state, Cmd cmd);
            ValueTask Update(TState state, Cmd cmd);
            ValueTask End(Cmd cmd);
        }

        public class Output {
            public HashSet<MemKey> Premises { get; } = new ();
            public Dictionary<MemId, Mem> Conclusions { get; } = new ();
            public Dictionary<MemId, Mem> DerivedFacts { get; } = new ();
            public List<TSideEffect> SideEffects { get; } = new ();
        }
        
        
        // TODO: consider using of arbitrary constants for making logic reproducible even it needs RNG or new GUIDS
        //       receive the constants from end()
        //       store the constants in the reflection
        //       pass the constants to the begin() if reflection has been already happened
        public async Task<Result<Output>> Run(Mem fact, CancellationToken ct = default) {
            var mems = new Dictionary<MemId, MemHolder>{[fact.Id] = MemHolder.Ok(fact)};
            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd>();
            var retrievingTasks = new Dictionary<MemId, ValueTask<Result<MemStamp>>>();
            var output = new Output();
            var iteration = 0;

            try {
                var (state, cmd) = _logic.Begin(fact);
                if (_tracer != null) await _tracer.Begin(state, cmd);
                ProcessCommand(cmd);

                while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                    if (iteration++ > _iterationsLimit) ThrowIterationsLimitException();
                    ct.ThrowIfCancellationRequested();
                    
                    // TODO: consider simultaneous execution 
                    // TODO: consider continue work after exception in update()
                    while (pendingMessages.TryDequeue(out var msg)) {
                        (state, cmd) = _logic.Update(state, msg);
                        if (_tracer != null) await _tracer.Update(state, cmd);
                        ProcessCommand(cmd);
                    }

                    ProcessPendingCommands();
                    if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                        await Task.WhenAny(retrievingTasks.Values.Select(x => x.AsTask()));
                        ProcessPendingCommands();
                    }
                }

                cmd = _logic.End(state);
                if (_tracer != null) await _tracer.End(cmd);
                ProcessCommand(cmd);
                return new Result<Output>(output);
            }
            catch (Exception e) {
                return Result<Output>.Exception(e);
            }
            
            void ProcessCommand(Cmd aCmd) {
                switch (aCmd) {
                    case Cmd.NoneCmd:
                        break;
                    case Cmd.MsgCmd x:
                        pendingMessages.Enqueue(x.Msg);
                        break;
                    case Cmd.BatchCmd x:
                        foreach (var item in x.Items) ProcessCommand(item);
                        break;
                    case Cmd.BatchOutputCmd x:
                        foreach (var item in x.Items) ProcessCommand(item);
                        break;
                    case Cmd.ConclusionCmd x:
                        output.Conclusions[x.Mem.Id] = x.Mem;
                        mems[x.Mem.Id] = MemHolder.Ok(x.Mem);
                        break;
                    case Cmd.DerivedFactCmd x:
                        output.DerivedFacts[x.Mem.Id] = x.Mem;
                        mems[x.Mem.Id] = MemHolder.Ok(x.Mem);
                        break;
                    case Cmd.SideEffectCmd x:
                        output.SideEffects.Add(x.SideEffect);
                        break;
                    case Cmd.RetrieveCmd x:
                        if (!TryCompleteRetrieve(x)) {
                            pendingCommands.AddLast(x);
                            StartRetrievingTask(x.MemId);
                        }
                        break;
                    case Cmd.RetrieveManyCmd x:
                        if (!TryCompleteRetrieveMany(x)) {
                            pendingCommands.AddLast(x);
                            foreach(var memId in x.MemIds)
                                StartRetrievingTask(memId);
                        }
                        break;
                }
            }

            bool TryCompleteRetrieve(Cmd.RetrieveCmd aCmd) {
                if (!mems!.TryGetValue(aCmd.MemId, out var holder)) return false;
                
                pendingMessages!.Enqueue(aCmd.Action(holder));
                return true;
            }

            bool TryCompleteRetrieveMany(Cmd.RetrieveManyCmd aCmd) {
                if (aCmd.MemIds.Any(x => !mems!.ContainsKey(x))) return false;

                var holders = aCmd.MemIds.Select(memId => mems![memId]).ToArray();
                pendingMessages!.Enqueue(aCmd.Action(holders));
                return true;
            }

            void StartRetrievingTask(MemId memId) {
                if (!retrievingTasks.ContainsKey(memId)) retrievingTasks[memId] = _retrieve(memId, ct);
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
                                RegisterPremise(memId, Result<MemStamp>.Exception(x));
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
                        Cmd.RetrieveCmd x => TryCompleteRetrieve(x),
                        Cmd.RetrieveManyCmd x => TryCompleteRetrieveMany(x),
                        _ => true}) {
                        pendingCommands.Remove(node);
                    }
                    node = node.Next;
                }
            }
                
            void RegisterPremise(MemId memId, Result<MemStamp> result) {
                if (!mems!.ContainsKey(memId)) {
                    mems[memId] = result.IsOk
                        ? MemHolder.Ok(result.Value.Mem)
                        : result.Error == Error.NotFound
                            ? MemHolder.NotExist
                            : MemHolder.NotAvailable;
                    
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