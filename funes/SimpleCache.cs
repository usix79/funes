using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    
    public class SimpleCache : ICache {
        
        private readonly ConcurrentDictionary<MemId, object> _memories = new();
        
        public ValueTask<Result<bool>> Put<T>(IEnumerable<Mem<T>> mems, int ttl, IRepository.Encoder<T> encoder) {

            foreach (var mem in mems) {
                _memories[mem.Key.Id] = mem;
            }
            
            return ValueTask.FromResult(new Result<bool>(true));
        }

        public ValueTask<Result<Mem<T>>[]> Get<T>(MemId[] ids, IRepository.Decoder<T> decoder) {
            var results = new Result<Mem<T>>[ids.Length];
            for (var i = 0; i < ids.Length; i++) {
                results[i] =
                    _memories.TryGetValue(ids[i], out var mem)
                        ? new Result<Mem<T>>((Mem<T>) mem)
                        : Result<Mem<T>>.NotFound;
            }
            
            return ValueTask.FromResult<Result<Mem<T>>[]>(results);
        }
    }
}