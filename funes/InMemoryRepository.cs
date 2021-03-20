using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {

    /// <summary>
    /// InMemory Repository for testing purposes
    /// </summary>
    public class InMemoryRepository : IRepository {

        private readonly ConcurrentDictionary<MemId, ConcurrentDictionary<ReflectionId, object>> _memories = new();

        public ValueTask<Result<bool>> Put<T>(Mem<T> mem, ReflectionId rid, IRepository.Encoder<T> _) {
            
            if (!_memories.ContainsKey(mem.Id)) {
                _memories.TryAdd(mem.Id, new ConcurrentDictionary<ReflectionId, object>());
            }

            _memories[mem.Id][rid] = mem;

            return ValueTask.FromResult(new Result<bool>(true));
        }

        public ValueTask<Result<Mem<T>>> Get<T>(MemId id, ReflectionId rid, IRepository.Decoder<T> _) {
            var result =
                _memories.TryGetValue(id, out var dict)
                    ? dict.TryGetValue(rid, out var mem) 
                        ? new Result<Mem<T>>((Mem<T>)mem) : Result<Mem<T>>.MemNotFound
                    : Result<Mem<T>>.MemNotFound;

            return ValueTask.FromResult(result);
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
            var result =
                _memories.TryGetValue(id, out var dict)
                    ? dict!.Keys
                        .OrderBy(rid => rid.Id)
                        .SkipWhile(rid => string.CompareOrdinal(rid.Id, before.Id) <= 0)
                        .Take(maxCount)
                    : Enumerable.Empty<ReflectionId>();

            return ValueTask.FromResult(new Result<IEnumerable<ReflectionId>>(result));
        }
    }
}