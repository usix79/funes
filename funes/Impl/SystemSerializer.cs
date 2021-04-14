using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading.Tasks;
using Funes.Indexes;

namespace Funes.Impl {
    public class SystemSerializer : ISerializer {
        
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            if (Increment.IsIncrement(eid)) return await EncodeJson(output, content);
            if (Increment.IsChild(eid)) return new Result<string>("");
            return Result<string>.SerdeError($"Not supported category: {eid.GetCategory()}");
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            if (Increment.IsIncrement(eid)) return await DecodeJson<Increment>(input, encoding);
            if (Increment.IsChild(eid)) return new Result<object>(null!);
            return Result<object>.SerdeError($"Not supported category: {eid.GetCategory()}");
        }
        
        public static async ValueTask<Result<string>> EncodeJson(Stream output, object cognition) {
            try {
                await JsonSerializer.SerializeAsync(output, cognition);
                return new Result<string>("json");
            }
            catch (Exception e) {
                return Result<string>.SerdeError(e.Message);
            }
        }

        public static async ValueTask<Result<object>> DecodeJson<T>(Stream input, string encoding) {
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