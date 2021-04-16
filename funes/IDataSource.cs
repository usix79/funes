using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataSource {
        ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer serializer, CancellationToken ct);
        
        ValueTask<Result<EventLog>> RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct);
        
    }
}