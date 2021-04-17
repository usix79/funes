using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes {
    
    public class LogicState<TMsg, TSideEffect> {
        public Dictionary<EntityId, EntityEntry> Entities { get; } = new ();
        public Dictionary<string, SetSnapshot> Sets { get; }= new ();
        public Queue<TMsg> PendingMessages { get; } = new ();
        public LinkedList<Cmd<TMsg, TSideEffect>> PendingCommands = new ();
        public Dictionary<EntityId, Task<Result<EntityEntry>>> RetrievingTasks = new ();
        public Dictionary<string, Task<Result<SetSnapshot>>> RetrievingSetTasks = new ();

        public void Reset() {
            Entities.Clear();
            Sets.Clear();
            PendingMessages.Clear();
            PendingCommands.Clear();
            RetrievingTasks.Clear();
            RetrievingSetTasks.Clear();
        }

        public bool InProcessing => PendingMessages.Count > 0 || PendingCommands.First != null;

        public bool ShouldWait => PendingMessages.Count == 0 && PendingCommands.First != null;

        public int TotalPendingTasks => RetrievingTasks.Count + RetrievingSetTasks.Count;

        public async Task WhenAnyPendingTasks(CancellationToken ct) {
            if (TotalPendingTasks == 0) return;
            
            var arr = ArrayPool<Task>.Shared.Rent(TotalPendingTasks);
            var idx = 0;
            foreach (var task in RetrievingTasks.Values) arr[idx++] = task;
            foreach (var task in RetrievingSetTasks.Values) arr[idx++] = task;
            while (idx < arr.Length) arr[idx++] = arr[0];

            try {
                await Task.WhenAny(arr);
            }
            finally {
                ArrayPool<Task>.Shared.Return(arr);
            }
        }
        
        public void RegisterEntity(EntityId eid, Result<EntityEntry> result) {
            if (!Entities.ContainsKey(eid)) {
                Entities[eid] = result.IsOk ? result.Value : EntityEntry.NotAvailable(eid);
            }
        }
        
    }
}