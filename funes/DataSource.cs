using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public class DataSource {
        
        private readonly IDataEngine _dataEngine;

        public DataSource(IDataEngine dataEngine) => _dataEngine = dataEngine;

        public ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct) {
            return _dataEngine.Retrieve(eid, ct);
        }

        public ValueTask<Result<EventLog>>
            RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct) {
            return _dataEngine.RetrieveEventLog(recordsId, offsetId, ct);
        }
        
    }
}