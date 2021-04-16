using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes.Impl {
    public class SystemSerializer : ISerializer {
        
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content, CancellationToken ct) {
            if (Increment.IsIncrement(eid)) return await Increment.Encode(output, (Increment)content);
            if (Increment.IsChild(eid)) return new Result<string>("");
            if (SetsHelpers.IsSnapshot(eid)) return await SetsHelpers.Encode(output, (SetSnapshot)content);
            return Result<string>.SerdeError($"Not supported entity: {eid}");
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding, CancellationToken ct) {
            if (Increment.IsIncrement(eid)) return await Increment.Decode(input, encoding);
            if (Increment.IsChild(eid)) return new Result<object>(null!);
            if (SetsHelpers.IsSnapshot(eid)) return await SetsHelpers.Decode(input, encoding, ct);
            return Result<object>.SerdeError($"Not supported entity: {eid}");
        }
    }
}