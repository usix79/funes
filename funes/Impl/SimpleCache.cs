using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    public class SimpleCache : ICache {
        private readonly ConcurrentDictionary<EntityId, (CognitionId, MemoryStream?, string)> _data = new();
        public async Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_data.TryGetValue(eid, out var triple)) return Result<EntityEntry>.NotFound;

            if (triple.Item2 == null) return new Result<EntityEntry>(EntityEntry.NotExist);
            
            triple.Item2.Position = 0;
            var serResult = await ser.Decode(triple.Item2, eid, triple.Item3);
            if (serResult.IsError) return new Result<EntityEntry>(serResult.Error);

            return new Result<EntityEntry>(EntityEntry.Ok(new Entity(eid, serResult.Value), triple.Item1));
        }

        public async Task<Result<bool>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            MemoryStream? stream = null;
            string encoding = "";

            if (entry.IsOk) {
                stream = new MemoryStream();
                var serResult = await ser.Encode(stream, entry.Eid, entry.Value);
                if (serResult.IsError) return new Result<bool>(serResult.Error);
                encoding = serResult.Value;
            }

            _data[entry.Eid] = (entry.Cid, stream, encoding);
            return new Result<bool>(true);
        }

        public async Task<Result<bool>> UpdateIfOlder(IEnumerable<EntityEntry> entries, ISerializer ser, CancellationToken ct) {

            bool result = true;
            
            foreach (var entry in entries) {
                ct.ThrowIfCancellationRequested();
                
                (MemoryStream? stream, string encoding) = (null, "");
                if (_data.TryGetValue(entry.Eid, out var triple)) {
                    if (!entry.Cid.IsOlderThan(triple.Item1)){
                        result = false;
                        continue;
                    }
                }

                if (entry.IsOk) {
                    stream = new MemoryStream();
                    var serResult = await ser.Encode(stream, entry.Eid, entry.Value);
                    if (serResult.IsError) return new Result<bool>(serResult.Error);
                    encoding = serResult.Value;
                }

                _data[entry.Eid] = (entry.Cid, stream, encoding);
            }
            return new Result<bool>(result);
        }
    }
}