using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public class DataSource {
        
        private readonly IDataEngine _dataEngine;

        private readonly ConcurrentDictionary<EntityId, ValueTask<Result<BinaryStamp>>> _tasks = new();
        
        public DataSource(IDataEngine dataEngine) => _dataEngine = dataEngine;

        public BinaryStamp[] GetRetrievedStamps() =>
            _tasks.Values
                .Where(task => task.IsCompletedSuccessfully && task.Result.IsOk)
                .Select(task => task.Result.Value)
                .ToArray();

        public ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct) {
            if (!_tasks.TryGetValue(eid, out var task)) {
                task = _dataEngine.Retrieve(eid, ct);
                _tasks[eid] = task;
            }
            
            return task;
        }

        public ValueTask<Result<EventLog>> RetrieveEventLog(
            EntityId recordsId, EntityId offsetId, CancellationToken ct) {
            return _dataEngine.RetrieveEventLog(recordsId, offsetId, ct);
        }
    }
}