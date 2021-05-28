using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class SimpleRepository : IRepository {

        private readonly ConcurrentDictionary<StampKey, BinaryStamp> _data = new();

        public Task<Result<Void>> Save(BinaryStamp stamp, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            _data[stamp.Key] = stamp;

            return Task.FromResult(new Result<Void>(Void.Value));
        }

        public Task<Result<BinaryStamp>> Load(StampKey key, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_data.TryGetValue(key, out var stamp)) 
                return Task.FromResult(Result<BinaryStamp>.NotFound);

            return Task.FromResult(new Result<BinaryStamp>(stamp));
        }
        
        public Task<Result<IncrementId[]>> HistoryBefore(EntityId entId,
            IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            var result =
                _data.Keys
                    .Where(key => key.EntId == entId && before.CompareTo(key.IncId) < 0)
                    .OrderBy(key => key.IncId)
                    .Take(maxCount)
                    .Select(key => key.IncId);

            return Task.FromResult(new Result<IncrementId[]>(result.ToArray()));
        }

        public Task<Result<IncrementId[]>> HistoryAfter(EntityId entId,
            IncrementId after, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            var result =
                _data.Keys
                    .Where(key => key.EntId == entId && string.CompareOrdinal(key.IncId.Id, after.Id) < 0)
                    .OrderByDescending(key => key.IncId)
                    .Select(key => key.IncId);

            return Task.FromResult(new Result<IncrementId[]>(result.ToArray()));
        }

        public Task<Result<string[]>> List(string category, string after = "", int maxCount = 1000, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();
            var catWithDel = category == "" ? category : category + "/";
            var items =
                _data.Keys
                    .Where(key => key.EntId.Id.StartsWith(catWithDel))
                    .Select(key => key.EntId.Id.Substring(catWithDel.Length))
                    .OrderBy(item => item)
                    .Select(item => {
                        var idx = item.IndexOf('/');
                        return idx != -1 ? item.Substring(0, idx) : item; })                    
                    .SkipWhile(item => after != "" && string.CompareOrdinal(item, after) <= 0)
                    .Distinct()
                    .Take(maxCount)
                    .ToArray();
            
            
            return Task.FromResult(new Result<string[]>(items));
        }
    }
}