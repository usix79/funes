using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class DefaultDataEngine: IDataEngine {

        private readonly TimeSpan _ttl;
        private readonly MemoryCache _memoryCache = new MemoryCache("FunesDefaultDataEngine");
        private readonly IRepository _repo;
        private readonly ICache _externalCache;
        private readonly ISourceOfTruth _sot;

        public DefaultDataEngine(IRepository repo, ICache cache, ISourceOfTruth sot, int ttlSeconds = 3600) {
            (_repo, _externalCache, _sot, _ttl) = (repo, cache, sot, TimeSpan.FromSeconds(ttlSeconds));
        }

        public async ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer serializer, CancellationToken ct) {

            var actualCidResult = Result<CognitionId>.NotFound;
            
            if (_memoryCache[eid.Id] is EntityEntry entry) {
                actualCidResult = await _externalCache.Contains(eid, ct);
                if (actualCidResult.IsOk && actualCidResult.Value == entry.Cid)
                    return new Result<EntityEntry>(entry);
            }

            var cacheResult = await _externalCache.Get(eid, serializer, ct);
            if (cacheResult.IsOk) {
                var newEntry = EntityEntry.Ok(cacheResult.Value.Entity, cacheResult.Value.Cid);
                _memoryCache.Set(eid.Id, newEntry, new CacheItemPolicy {SlidingExpiration = _ttl});
                return new Result<EntityEntry>(newEntry);
            }

            // todo call sot
            if (actualCidResult.IsError) {
                var historyResult = await _repo.GetHistory(eid, CognitionId.Singularity);
                if (historyResult.IsOk) {
                    var cids = historyResult.Value as CognitionId[] ?? historyResult.Value.ToArray();
                    if (cids.Length == 0) {
                        var newEntry = EntityEntry.NotExist;
                        return new Result<EntityEntry>(newEntry);
                    }

                    actualCidResult = new Result<CognitionId>(cids[0]);
                }
            }

            var repoResult = await _repo.Get(new EntityStampKey(eid, actualCidResult.Value), serializer);

            if (repoResult.IsOk) {
                var newEntry = EntityEntry.Ok(repoResult.Value.Entity, repoResult.Value.Cid);
                _memoryCache.Set(eid.Id, newEntry, new CacheItemPolicy {SlidingExpiration = _ttl});
                //await _externalCache.Set(new EntityStamp[]{})
                return new Result<EntityEntry>(newEntry);
            }

            return new Result<EntityEntry>(EntityEntry.NotAvailable);
        }
        
        public Task<Result<bool>> Upload(
            IEnumerable<EntityStamp> mems, 
            ISerializer serializer, CancellationToken ct, 
            bool skipCache = false) {
            throw new System.NotImplementedException();
        }

        public ValueTask<Result<CognitionId>> GetActualCid(EntityId id, CancellationToken ct) {
            throw new System.NotImplementedException();
        }

        public Task<Result<IDataEngine.CommitDetail[]>> Commit(
            IEnumerable<EntityStampKey> premises, 
            IEnumerable<EntityStampKey> conclusions, 
            CancellationToken ct) {
            throw new System.NotImplementedException();
        }

        public Task<Result<bool>> Rollback(IEnumerable<IDataEngine.CommitDetail> commitResults, CancellationToken ct) {
            throw new System.NotImplementedException();
        }

    }
}