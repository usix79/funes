using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualBasic;

namespace Funes {
    
    public class SimpleTruthSource : ITruthSource {

        private readonly ConcurrentDictionary<MemId, ReflectionId> _latest = new();
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1); 
        
        public ValueTask<Result<ReflectionId>> GetActualConclusion(MemId id) {
            var result =
                _latest.TryGetValue(id, out var rid)
                    ? new Result<ReflectionId>(rid)
                    : Result<ReflectionId>.NotFound;
            return ValueTask.FromResult<Result<ReflectionId>>(result);
        }

        public ValueTask<Result<ReflectionId>[]> GetActualConclusions(IEnumerable<MemId> ids) {
            var idsArray = ids as MemId[] ?? ids.ToArray();
            var results = new Result<ReflectionId>[idsArray.Length];
            for (var i = 0; i < idsArray.Length; i++) {
                results[i] =
                    _latest.TryGetValue(idsArray[i], out var rid)
                        ? new Result<ReflectionId>(rid)
                        : Result<ReflectionId>.NotFound;
            }
            
            return ValueTask.FromResult(results);
        }

        public async ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions) {
            await _lock.WaitAsync();
            try {
                var premisesKeys = premises as MemKey[] ?? premises.ToArray();
                var premisesIds = premisesKeys.Select(key => key.Id).ToArray();
                var actualConclusions = await GetActualConclusions(premisesIds);

                for (var i = 0; i < premisesKeys.Length; i++){
                    var actual = actualConclusions[i];
                    if (actual.IsOk && actual.Value != premisesKeys[i].Rid 
                        || actual.IsError && actual.Error != Error.NotFound) {
                        return new Result<bool>(false);
                    }
                }
                
                foreach (var conclusion in conclusions) {
                    _latest[conclusion.Id] = conclusion.Rid;
                }

                return new Result<bool>(true);
            }
            finally {
                _lock.Release();
            }
        }
    }
}