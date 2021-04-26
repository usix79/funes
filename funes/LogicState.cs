using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    
    public class LogicState<TMsg, TSideEffect> {
        public Queue<TMsg> PendingMessages { get; } = new ();
        public LinkedList<Cmd<TMsg, TSideEffect>> PendingCommands { get; } = new ();
        public Dictionary<EntityId, Task> PendingTasks { get; } = new();

        public void Reset() {
            PendingMessages.Clear();
            PendingCommands.Clear();
            PendingTasks.Clear();
        }

        public bool InProcessing => PendingMessages.Count > 0 || PendingCommands.First != null;
        public bool ShouldWait => PendingMessages.Count == 0 && PendingCommands.First != null;
        
        public async Task WhenAnyPendingTasks(CancellationToken ct) {
            if (PendingTasks.Count == 0) return;
            
            var keysArr = ArrayPool<EntityId>.Shared.Rent(PendingTasks.Count);
            var tasksArr = ArrayPool<Task>.Shared.Rent(PendingTasks.Count);
            var idx = 0;
            foreach (var pair in PendingTasks) {
                keysArr[idx] = pair.Key;
                tasksArr[idx] = pair.Value;
                idx++;
            }

            while (idx < tasksArr.Length) {
                keysArr[idx] = keysArr[0];
                tasksArr[idx] = tasksArr[0];
                idx++;
            }

            try {
                await Task.WhenAny(tasksArr);
                
                // remove finished tasks
                for (var i = 0; i < PendingTasks.Count; i++) {
                    if (tasksArr[i].IsCompleted)
                        PendingTasks.Remove(keysArr[i]);
                }
            }
            finally {
                ArrayPool<Task>.Shared.Return(tasksArr);
                ArrayPool<EntityId>.Shared.Return(keysArr);
            }
        }
        
    }
}