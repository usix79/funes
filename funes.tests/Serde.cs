using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes.Tests {
    
    public static class Serde {
        public static async ValueTask<Result<string>> Encoder(Stream output, object content) {
            try {
                await JsonSerializer.SerializeAsync(output, content);
                return new Result<string>("json");
            }
            catch (Exception ex) {
                return Result<string>.Exception(ex);
            }
        }

        public static async ValueTask<Result<object>> Decoder<T>(Stream input, string encoding) {
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