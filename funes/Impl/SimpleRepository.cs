using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Toolkit.HighPerformance;

namespace Funes.Impl {
    
    public class SimpleRepository : IRepository {

        private readonly ConcurrentDictionary<EntityStampKey, (string, ReadOnlyMemory<byte>)> _data = new();

        public async ValueTask<Result<Void>> Save(EntityStamp stamp, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var stream = new MemoryStream();
            var serResult = await ser.Encode(stream, stamp.EntId, stamp.Value);
            if (serResult.IsError) return new Result<Void>(serResult.Error);

            if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
            _data[stamp.Key] = (serResult.Value, buffer);

            return new Result<Void>(Void.Value);
        }

        public async ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_data.TryGetValue(key, out var pair)) return Result<EntityStamp>.NotFound;

            var stream =  pair.Item2.AsStream();
            var serResult = await ser.Decode(stream, key.EntId, pair.Item1);
            if (serResult.IsError) return new Result<EntityStamp>(serResult.Error);

            return new Result<EntityStamp>(new EntityStamp(key, serResult.Value));
        }

        public ValueTask<Result<Void>> SaveEvent(EntityId eid, Event evt, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            _data[eid.CreateStampKey(evt.IncId)] = ("", evt.Data);
            return ValueTask.FromResult(new Result<Void>(Void.Value));
        }

        public ValueTask<Result<Event>> LoadEvent(EntityStampKey key, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_data.TryGetValue(key, out var pair)) 
                return ValueTask.FromResult(Result<Event>.NotFound);

            var evt = new Event(key.IncId, pair.Item2);
            return ValueTask.FromResult(new Result<Event>(evt));
        }

        public ValueTask<Result<IncrementId[]>> HistoryBefore(EntityId entId, 
            IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            var result =
                _data.Keys
                    .Where(key => key.EntId == entId && before.CompareTo(key.IncId) < 0)
                    .OrderBy(key => key.IncId)
                    .Take(maxCount)
                    .Select(key => key.IncId);

            return ValueTask.FromResult(new Result<IncrementId[]>(result.ToArray()));
        }

        public ValueTask<Result<IncrementId[]>> HistoryAfter(EntityId entId, 
            IncrementId after, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            var result =
                _data.Keys
                    .Where(key => key.EntId == entId && string.CompareOrdinal(key.IncId.Id, after.Id) < 0)
                    .OrderByDescending(key => key.IncId)
                    .Select(key => key.IncId);

            return ValueTask.FromResult(new Result<IncrementId[]>(result.ToArray()));
        }
    }
}