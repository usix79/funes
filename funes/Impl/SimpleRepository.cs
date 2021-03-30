using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class SimpleRepository : IRepository {

        private readonly ConcurrentDictionary<EntityStampKey, (string, MemoryStream)> _data = new();

        public async ValueTask<Result<bool>> Save(EntityStamp stamp, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var stream = new MemoryStream();
            var serResult = await ser.Encode(stream, stamp.Eid, stamp.Value);
            if (serResult.IsError) return new Result<bool>(serResult.Error);
            
            _data[stamp.Key] = (serResult.Value, stream);

            return new Result<bool>(true);
        }

        public async ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_data.TryGetValue(key, out var pair)) return Result<EntityStamp>.NotFound;

            pair.Item2.Position = 0;
            var serResult = await ser.Decode(pair.Item2, key.Eid, pair.Item1);
            if (serResult.IsError) return new Result<EntityStamp>(serResult.Error);

            return new Result<EntityStamp>(new EntityStamp(key, serResult.Value));
        }
        
        public ValueTask<Result<IEnumerable<CognitionId>>> History(EntityId id, 
            CognitionId before, int maxCount = 1, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            var result =
                _data.Keys
                    .Where(key => key.Eid == id)
                    .OrderBy(key => key.Cid)
                    .SkipWhile(key => string.CompareOrdinal(key.Cid.Id, before.Id) <= 0)
                    .Take(maxCount)
                    .Select(key => key.Cid);

            return ValueTask.FromResult(new Result<IEnumerable<CognitionId>>(result));
        }
    }
}