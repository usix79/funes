using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct);
        Task<Result<Void>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct);
        Task<Result<bool>> UpdateIfNewer(IEnumerable<EntityEntry> entries, ISerializer ser, CancellationToken ct);
    }
}