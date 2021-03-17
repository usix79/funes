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

        private readonly ConcurrentDictionary<MemKey, ReflectionId> _latest = new();
        private readonly ConcurrentDictionary<MemKey, ConcurrentDictionary<ReflectionId, Mem>> _storage = new();

        public Task<ReflectionId> GetLatestRid(MemKey key) {
            var result = _latest.TryGetValue(key, out var rid) ? rid : ReflectionId.Null;
            return Task.FromResult(result);
        }

        public Task SetLatestRid(MemKey key, ReflectionId rid) {
            _latest[key] = rid;
            return Task.CompletedTask;
        }

        public Task<Mem?> GetMem(MemKey key, ReflectionId reflectionId) {
            var result =
                _storage.TryGetValue(key, out var dict)
                    ? dict.TryGetValue(reflectionId, out var mem) ? mem : null
                    : null;

            return Task.FromResult(result);
        }

        public Task PutMem(Mem mem, ReflectionId reflectionId) {
            if (!_storage.ContainsKey(mem.Key)) {
                _storage.TryAdd(mem.Key, new ConcurrentDictionary<ReflectionId, Mem>());
            }

            _storage[mem.Key][reflectionId] = mem;

            return Task.CompletedTask;
        }

        public Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1) {
            var result =
                _storage.TryGetValue(key, out var dict)
                    ? dict!.Keys
                        .OrderBy(rid => rid.Id)
                        .SkipWhile(rid => string.CompareOrdinal(rid.Id, before.Id) <= 0)
                        .Take(maxCount)
                    : Enumerable.Empty<ReflectionId>();

            return Task.FromResult(result);
        }
    }
}