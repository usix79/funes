using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {

    /// <summary>
    /// InMemory Repository for testing purpose
    /// </summary>
    public class SimpleRepository : IRepository {

        private readonly ConcurrentDictionary<MemKey, object> _memories = new();

        public ValueTask<Result<bool>> Put<T>(Mem<T> mem, IRepository.Encoder<T> _) {

            _memories[mem.Key] = mem;

            return ValueTask.FromResult(new Result<bool>(true));
        }

        public ValueTask<Result<Mem<T>>> Get<T>(MemKey key, IRepository.Decoder<T> _) {
            var result =
                _memories.TryGetValue(key, out var mem)
                    ? new Result<Mem<T>>((Mem<T>) mem)
                    : Result<Mem<T>>.NotFound;

            return ValueTask.FromResult(result);
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
            var result =
                _memories.Keys
                    .Where(key => key.Id == id)
                    .OrderBy(key => key.Rid)
                    .SkipWhile(key => string.CompareOrdinal(key.Rid.Id, before.Id) <= 0)
                    .Take(maxCount)
                    .Select(key => key.Rid);

            return ValueTask.FromResult(new Result<IEnumerable<ReflectionId>>(result));
        }
    }
}