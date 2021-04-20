using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataSource {
        ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct);
        
        ValueTask<Result<EventLog>> RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct);
        
    }
}