using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes.Impl {
    public class SystemSerializer : ISerializer {

        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            return
                eid.GetCategory() switch {
                    Increment.Category => await JsonEncode(output, content),
                    Increment.ChildrenCategory => new Result<string>(""),
                    _ => Result<string>.SerdeError($"Not supported category: {eid.GetCategory()}")
                };
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            return
                eid.GetCategory() switch {
                    Increment.Category => await JsonDecode<Increment>(input, encoding),
                    Increment.ChildrenCategory => new Result<object>(null!),
                    _ => Result<object>.SerdeError($"Not supported category: {eid.GetCategory()}")
                };
        }
        
        public static async ValueTask<Result<string>> JsonEncode(Stream output, object cognition) {
            try {
                await JsonSerializer.SerializeAsync(output, cognition);
                return new Result<string>("json");
            }
            catch (Exception e) {
                return Result<string>.SerdeError(e.Message);
            }
        }

        public static async ValueTask<Result<object>> JsonDecode<T>(Stream input, string encoding) {
            if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);
            try {
                var reflectionOrNull = await JsonSerializer.DeserializeAsync<T>(input);
                return reflectionOrNull != null
                    ? new Result<object>(reflectionOrNull)
                    : Result<object>.SerdeError("null");
            }
            catch (Exception e) {
                return Result<object>.SerdeError(e.Message);
            }
        }
        
    }
}