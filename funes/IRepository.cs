using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        Task<(Mem,ReflectionId)?> GetLatest(MemKey key);
        Task<Mem?> Get(MemKey key, ReflectionId reflectionId);
        Task Put(Mem mem, ReflectionId reflectionId);
        Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1);
    }
}