using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public interface ISourceOfTruth {
        
        ValueTask<Result<ReflectionId>> GetActualConclusion(MemId id);
 
        ValueTask<Result<ReflectionId>[]> GetActualConclusions(IEnumerable<MemId> ids);
        
        ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions);
    }
}