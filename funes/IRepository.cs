using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        ValueTask<Result<bool>> Put(EntityStamp entityStamp, ISerializer serializer);
        ValueTask<Result<EntityStamp>> Get(EntityStampKey key, ISerializer serializer);
        ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(EntityId id, ReflectionId before, int maxCount = 1);
    }
}