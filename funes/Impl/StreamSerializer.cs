using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.IO;

namespace Funes.Impl {
    public class StreamSerializer: ISerializer {
        
        private static readonly RecyclableMemoryStreamManager StreamManager = new ();

        private EntityId _targetEntId = EntityId.None;
        private RecyclableMemoryStream? _stream = null;
        private string _encoding = "";
        
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            if (_targetEntId == eid && _stream != null) {
                foreach (var memory in _stream.GetReadOnlySequence()) {
                    await output.WriteAsync(memory);
                }                
                return new Result<string>(_encoding);
            }
            
            return Result<string>.SerdeError($"Entity was not decoded before {eid}");
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            var stream = (RecyclableMemoryStream)StreamManager.GetStream();
            await input.CopyToAsync(stream);

            (_targetEntId, _encoding, _stream) = (eid, encoding, stream);

            return new Result<object>(stream);
        }

        public async ValueTask<Result<string>> EncodeForReal(EntityId eid, object content, ISerializer ser) {
            var stream = (RecyclableMemoryStream)StreamManager.GetStream();
            var encodingResult = await ser.Encode(stream, eid, content);

            if (encodingResult.IsOk) {
                (_targetEntId, _encoding, _stream) = (eid, encodingResult.Value, stream);
            }
            
            return encodingResult;
        }

        public async ValueTask<Result<object>> DecodeForReal(EntityId eid, ISerializer ser) {
            if (_targetEntId == eid && _stream != null) {
                _stream.Position = 0;
                return await ser.Decode(_stream, eid, _encoding);
            } 
            
            return Result<object>.SerdeError($"Entity was not decoded before {eid}");
        }

        public void Reset() {
            _stream?.Dispose();
            (_targetEntId, _encoding, _stream) = (EntityId.None, "", null);
        }
    }
}