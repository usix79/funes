using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<EntityEntry>> Get(EntityId eid, ISerializer serializer, CancellationToken ct);
        Task<Result<bool>> Update(IEnumerable<EntityEntry> entries, ISerializer serializer, CancellationToken ct);
        Task<Result<bool>> Set(IEnumerable<EntityEntry> entries, ISerializer serializer, CancellationToken ct);

    }
}