using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {
    
    public class SimpleCache : ICache {
        
        private readonly ConcurrentDictionary<MemId, Mem> _memories = new();
        
        public ValueTask<Result<bool>> Put(IEnumerable<Mem> mems, int ttl, IRepository.Encoder _) {

            foreach (var mem in mems) {
                _memories[mem.Key.Id] = mem;
            }
            
            return ValueTask.FromResult(new Result<bool>(true));
        }

        public ValueTask<Result<Mem>[]> Get(IEnumerable<(MemId, IRepository.Decoder)> ids) {
            var arr = ids as (MemId, IRepository.Decoder)[] ?? ids.ToArray();
            var results = new Result<Mem>[arr.Length];
            for (var i = 0; i < arr.Length; i++) {
                results[i] =
                    _memories.TryGetValue(arr[i].Item1, out var mem)
                        ? new Result<Mem>(mem)
                        : Result<Mem>.NotFound;
            }
            
            return ValueTask.FromResult(results);
        }
    }
}