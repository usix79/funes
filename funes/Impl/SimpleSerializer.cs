using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes.Impl {

    public class SimpleSerializer<T> : ISerializer {
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            try {
                await JsonSerializer.SerializeAsync(output, content);
                return new Result<string>("json");
            }
            catch (Exception ex) {
                return Result<string>.Exception(ex);
            }
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            try {
                if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);

                var content = await JsonSerializer.DeserializeAsync<T>(input);
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