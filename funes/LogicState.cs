using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Indexes;
using Funes.Sets;

namespace Funes {
    
    public class LogicState<TMsg, TSideEffect> {
        public Queue<TMsg> PendingMessages { get; } = new ();
        public LinkedList<Cmd<TMsg, TSideEffect>> PendingCommands { get; } = new ();
        public HashSet<Task> PendingTasks { get; } = new();
        
        public HashSet<EntityId> StartedRetrieving { get; } = new();
        public HashSet<string> StartedSetRetrieving { get; } = new();
        public Dictionary<int, Task<Result<IndexesModule.SelectionResult>>> StartedSelections { get; } = new();

        public void Reset() {
            PendingMessages.Clear();
            PendingCommands.Clear();
            PendingTasks.Clear();
            StartedRetrieving.Clear();
            StartedSetRetrieving.Clear();
            StartedSelections.Clear();
        }

        public bool InProcessing => PendingMessages.Count > 0 || PendingCommands.First != null;
        public bool ShouldWait => PendingMessages.Count == 0 && PendingCommands.First != null;
        
        public async Task WhenAnyPendingTasks(CancellationToken ct) {
            if (PendingTasks.Count == 0) return;
            
            var tasksArr = ArrayPool<Task>.Shared.Rent(PendingTasks.Count);
            var idx = 0;
            foreach (var task in PendingTasks) {
                tasksArr[idx] = task;
                idx++;
            }

            while (idx < tasksArr.Length) {
                tasksArr[idx] = tasksArr[0];
                idx++;
            }

            try {
                await Task.WhenAny(tasksArr);
                
                // remove finished tasks
                for (var i = 0; i < PendingTasks.Count; i++) {
                    if (tasksArr[i].IsCompleted)
                        PendingTasks.Remove(tasksArr[i]);
                }
            }
            finally {
                ArrayPool<Task>.Shared.Return(tasksArr);
            }
        }
        
    }
}