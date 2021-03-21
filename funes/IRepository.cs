using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Funes {
    
    public interface IRepository {
        public delegate ValueTask<Result<string>> Encoder<in T>(Stream output, T content);
        public delegate ValueTask<Result<T>> Decoder<T>(Stream input, string encoding);
        ValueTask<Result<bool>> Put<T>(Mem<T> mem, Encoder<T> encoder);
        ValueTask<Result<Mem<T>>> Get<T>(MemKey key, Decoder<T> decoder);
        ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1);
    }
}