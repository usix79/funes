using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        Task<ReflectionId> GetLatestRid(MemKey key);
        Task SetLatestRid(MemKey key, ReflectionId rid);
        Task<Mem?> GetMem(MemKey key, ReflectionId reflectionId);
        Task PutMem(Mem mem, ReflectionId reflectionId);
        Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1);
    }
}