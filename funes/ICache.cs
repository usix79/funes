using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<CognitionId>> Contains(EntityId id, CancellationToken ct);
        Task<Result<EntityStamp>> Get(EntityId eid, ISerializer serializer, CancellationToken ct);
        
        Task<Result<bool>> Set(IEnumerable<EntityStamp> stamps, ISerializer serializer, CancellationToken ct);
    }
}