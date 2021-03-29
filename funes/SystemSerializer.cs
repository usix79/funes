using System.IO;
using System.Threading.Tasks;

namespace Funes {
    public class SystemSerializer : ISerializer {

        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            return
                eid.Category switch {
                    Cognition.Category => await Cognition.Encoder(output, content),
                    Cognition.ChildrenCategory => new Result<string>(""),
                    _ => Result<string>.SerdeError($"Not supported category: {eid.Category}")
                };
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            return
                eid.Category switch {
                    Cognition.Category => await Cognition.Decoder(input, encoding),
                    Cognition.ChildrenCategory => new Result<object>(null!),
                    _ => Result<object>.SerdeError($"Not supported category: {eid.Category}")
                };
        }
    }
}