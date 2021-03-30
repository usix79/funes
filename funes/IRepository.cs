using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        ValueTask<Result<bool>> Save(EntityStamp stamp, ISerializer serializer, CancellationToken ct);
        ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer serializer, CancellationToken ct);
        ValueTask<Result<IEnumerable<CognitionId>>> History(EntityId eid, CognitionId before, int maxCount = 1, CancellationToken ct = default);
    }
}