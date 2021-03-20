using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes.Tests {
    
    public static class Serde {
        public static async ValueTask<Result<string>> Encoder<T>(Stream output, T content) {
            try {
                await JsonSerializer.SerializeAsync(output, content);
                return new Result<string>("json");
            }
            catch (Exception ex) {
                return Result<string>.Exception(ex);
            }
        }

        public static async ValueTask<Result<T>> Decoder<T>(Stream input, string encoding) {
            try {
                if ("json" != encoding) return Result<T>.NotSupportedEncoding(encoding);

                var content = await JsonSerializer.DeserializeAsync<T>(input);
                return content != null
                    ? new Result<T>(content)
                    : Result<T>.SerdeError($"Failed to deserialize instance of {typeof(T)}");
            }
            catch (Exception ex) {
                return Result<T>.Exception(ex);
            }
        }
    }
}