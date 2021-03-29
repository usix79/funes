using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        ValueTask<Result<bool>> Put(EntityStamp stamp, ISerializer serializer);
        ValueTask<Result<EntityStamp>> Get(EntityStampKey key, ISerializer serializer);
        ValueTask<Result<IEnumerable<CognitionId>>> GetHistory(EntityId eid, CognitionId before, int maxCount = 1);
    }
}