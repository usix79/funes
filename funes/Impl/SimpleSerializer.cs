using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Toolkit.HighPerformance;

namespace Funes.Impl {

    public class SimpleSerializer<T> : ISerializer {
        public Result<BinaryData> Encode(EntityId eid, object content) {
            try {
                using MemoryStream stream = new ();
                using Utf8JsonWriter writer = new (stream);
                JsonSerializer.Serialize<T>(writer, (T)content);
                if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                return new Result<BinaryData>(new BinaryData("json", buffer));
            }
            catch (Exception ex) {
                return Result<BinaryData>.Exception(ex);
            }
        }

        public Result<object> Decode(EntityId eid, BinaryData data) {
            try {
                if ("json" != data.Encoding) return Result<object>.NotSupportedEncoding(data.Encoding);

                var content = JsonSerializer.Deserialize<T>(data.Memory.Span);
                return content != null
                    ? new Result<object>(content)
                    : Result<object>.SerdeError($"Failed to deserialize instance of {typeof(T)}");
            }
            catch (Exception ex) {
                return Result<object>.Exception(ex);
            }
        }
    }
}