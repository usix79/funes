using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct);
        Task<Result<bool>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct);
        Task<Result<bool>> UpdateIfOlder(IEnumerable<EntityEntry> entries, ISerializer ser, CancellationToken ct);
    }
}