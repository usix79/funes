using System.IO;
using System.Threading.Tasks;

namespace Funes {
    public interface ISerializer {
        public ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content);
        public ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding);
    }
}