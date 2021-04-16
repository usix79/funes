using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {

    public class SimpleSerializer<T> : ISerializer {
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content, CancellationToken ct) {
            try {
                await JsonSerializer.SerializeAsync(output, content, cancellationToken: ct);
                return new Result<string>("json");
            }
            catch (Exception ex) {
                return Result<string>.Exception(ex);
            }
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding, CancellationToken ct) {
            try {
                if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);

                var content = await JsonSerializer.DeserializeAsync<T>(input, cancellationToken: ct);
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