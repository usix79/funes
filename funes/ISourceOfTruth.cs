using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public interface ISourceOfTruth {
        
        ValueTask<Result<ReflectionId>> GetActualRid(MemId id);
 
        ValueTask<Result<ReflectionId>[]> GetActualRids(IEnumerable<MemId> ids);
        
        ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions);
    }
}