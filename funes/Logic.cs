using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {
    
    public struct MemHolder {
        public Mem Mem { get; }
        public bool IsNotAvailable { get; init; }
        public bool IsNotExist { get; init; }
        public bool IsOk => !IsNotAvailable && !IsNotExist;

        private MemHolder(Mem mem) => (Mem, IsNotAvailable, IsNotExist) = (mem, false, false);
        public static MemHolder Ok(Mem mem) => new MemHolder(mem);
        public static MemHolder NotAvailable = new MemHolder {IsNotAvailable = true};
        public static MemHolder NotFound = new MemHolder {IsNotExist = true};
    }

    public class Logic<TState,TMsg,TSideEffect> {
        
        public abstract record Cmd {
            public record MsgCmd(TMsg Msg) : Cmd;
            public record RetrieveCmd(MemId MemId, Func<MemHolder, TMsg> Action) : Cmd;
            public record RetrieveManyCmd(MemId[] MemIds, Func<MemHolder[], TMsg> Action) : Cmd;
            public record BatchCmd(Cmd[] Items) : Cmd;
            public abstract record OutputCmd : Cmd;
            public record ConclusionCmd(Mem Mem) : OutputCmd;
            public record DerivedFactCmd(Mem Mem) : OutputCmd;
            public record SideEffectCmd(TSideEffect SideEffect) : OutputCmd;
            public record BatchOutputCmd(OutputCmd[] Items) : OutputCmd;
        }
        
        public delegate Task<Result<MemStamp>> Retrieve(MemId id);
        public delegate (TState, Cmd) Begin(Mem mem);
        public delegate (TState, Cmd) Update(TState state, TMsg msg);
        public delegate Cmd.OutputCmd End(TState state);

        public class Output {
            public HashSet<MemKey> Premises { get; } = new ();
            public Dictionary<MemId, Mem> Conclusions { get; } = new();
            public Dictionary<MemId, Mem> DerivedFacts { get; } = new();
            public List<TSideEffect> SideEffects { get; } = new();
        }
        
        public static async Task<Output> Run(Retrieve retrieve, Begin begin, Update update, End end, Mem fact) {
            var mems = new Dictionary<MemId, MemHolder>{[fact.Id] = MemHolder.Ok(fact)};
            var pendingMessages = new Queue<TMsg>();
            var pendingCommands = new LinkedList<Cmd>();
            var retrievingTasks = new Dictionary<MemId, Task<Result<MemStamp>>>();
            var output = new Output();
            
            var (state, cmd) = begin(fact);
            ProcessCommand(cmd);

            while (pendingMessages.Count > 0 || pendingCommands.First != null) {
                while (pendingMessages.TryDequeue(out var msg)) {
                    (state, cmd) = update(state, msg);
                    ProcessCommand(cmd);
                }

                ProcessPendingCommands();
                
                if (pendingMessages.Count == 0 && pendingCommands.First != null) {
                    await Task.WhenAny(retrievingTasks.Values);
                }
            }

            ProcessCommand(end(state));
            
            return output;

            void ProcessCommand(Cmd aCmd) {
                switch (aCmd) {
                    case Cmd.MsgCmd x:
                        pendingMessages.Enqueue(x.Msg);
                        break;
                    case Cmd.BatchCmd x:
                        Array.ForEach(x.Items, ProcessCommand);
                        break;
                    case Cmd.BatchOutputCmd x:
                        Array.ForEach(x.Items, ProcessCommand);
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
                            Array.ForEach(x.MemIds, StartRetrievingTask);
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
                if (!retrievingTasks.ContainsKey(memId)) {
                    retrievingTasks[memId] = retrieve(memId);
                }
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
                if (!mems.ContainsKey(memId)) {
                    var holder = result.IsOk
                        ? MemHolder.Ok(result.Value.Mem)
                        : result.Error == Error.NotFound
                            ? MemHolder.NotFound
                            : MemHolder.NotAvailable;


                    mems[memId] = holder;

                    if (result.IsOk) {
                        output.Premises.Add(result.Value.Key);
                    }
                }
            }
        }
    }
}