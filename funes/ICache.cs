using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public interface ICache {
        
        ValueTask<Result<bool>> Put<T>(IEnumerable<Mem<T>> mems, int ttl, IRepository.Encoder<T> encoder);
        
        ValueTask<Result<Mem<T>>[]> Get<T>(MemId[] ids, IRepository.Decoder<T> decoder);
    }
}