using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace Funes {
    public class StreamSerializer: ISerializer {
        
        private readonly ConcurrentDictionary<EntityId, (string, ArraySegment<byte>)> _streams = new();
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            if (_streams.TryGetValue(eid, out var pair)) {
                var stream = new MemoryStream(pair.Item2.Array!, pair.Item2.Offset, pair.Item2.Count);
                await stream.CopyToAsync(output);
                return new Result<string>(pair.Item1);
            }
            
            return Result<string>.SerdeError($"Entity was not decoded before {eid}");
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            var stream = new MemoryStream();
            await input.CopyToAsync(stream);
            
            if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
            _streams[eid] = (encoding, buffer);    
            
            return new Result<object>(stream);
        }

        public async ValueTask<Result<string>> EncodeForReal(EntityId eid, object content, ISerializer ser) {
            var stream = new MemoryStream();
            var encodingResult = await ser.Encode(stream, eid, content);
            if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray(); 
            _streams[eid] = (encodingResult.Value, buffer);    
            return encodingResult;
        }

        public async ValueTask<Result<object>> DecodeForReal(EntityId eid, ISerializer ser) {
            if (_streams.TryGetValue(eid, out var pair)) {
                var stream = new MemoryStream(pair.Item2.Array!, pair.Item2.Offset, pair.Item2.Count);
                return await ser.Decode(stream, eid, pair.Item1);
            }
            
            return Result<object>.SerdeError($"Entity was not decoded before {eid}");
        }
    }
}