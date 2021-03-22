using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Funes {
    
    public interface IRepository {
        public delegate ValueTask<Result<string>> Encoder(Stream output, object content);
        public delegate ValueTask<Result<object>> Decoder(Stream input, string encoding);

        ValueTask<Result<bool>> Put(Mem mem, Encoder encoder);
        ValueTask<Result<Mem>> Get(MemKey key, Decoder decoder);
        ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1);


        // TODO: future approach for reducing allocations
        // ValueTask<Result<bool>> Put(MemKey key, ArraySegment<byte> buffer, string encoding);
        // async ValueTask<Result<bool>> Put(Mem mem, Encoder encoder) {
        //     await using MemoryStream stream = new();
        //     var result = await encoder(stream, mem.Value);
        //     if (result.IsOk) {
        //         return await Put(mem.Key, stream.GetBuffer(), result.Value);
        //     }
        //
        //     return new Result<bool>(result.Error);
        // }
    }
}