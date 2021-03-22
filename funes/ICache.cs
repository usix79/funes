using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public interface ICache {
        
        ValueTask<Result<bool>> Put(IEnumerable<Mem> mems, int ttl, IRepository.Encoder encoder);
        
        ValueTask<Result<Mem>[]> Get(IEnumerable<(MemId, IRepository.Decoder)> ids);
    }
}