using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class StatelessDataEngine: IDataEngine {

        private readonly IRepository _repo;
        private readonly ICache _cache;
        private readonly ITransactionEngine _transaction;
        private ConcurrentBag<Task> _pendingTasks = new ();

        public StatelessDataEngine(IRepository repo, ICache cache, ITransactionEngine transaction) {
            (_repo, _cache, _transaction) = (repo, cache, transaction);
        }

        public async ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer ser, CancellationToken ct) {
            
            var cacheResult = await _cache.Get(eid, ser, ct);

            if (cacheResult.IsOk) return cacheResult;
            
            // miss cache, look in the repo
            var repoResult = await LoadActualStamp(eid, CognitionId.Singularity, ser, ct);

            if (repoResult.IsError && repoResult.Error != Error.NotFound)
                return new Result<EntityEntry>(repoResult.Error);

            var entry = repoResult.IsOk ? repoResult.Value.ToEntry() : EntityEntry.NotExist;
                
            // try set cache item
            var trySetResult = await _cache.Update(new[] {entry}, ser, ct);

            if (trySetResult.IsOk && !trySetResult.Value)
                // what? cache already has gotten a value, look in cache again 
                return await _cache.Get(eid, ser, ct);

            // TODO: log trySet Error

            return new Result<EntityEntry>(entry);
        }
        
        public async ValueTask<Result<bool>> Upload(
            IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct, bool skipCache = false) {
            var stampsArr = stamps as EntityStamp[] ?? stamps.ToArray();
                
            if (stampsArr.Length == 0) return new Result<bool>(false);
           
            var result = skipCache
                ? new Result<bool>(true)
                : await _cache.Update(stampsArr.Select(x => x.ToEntry()), ser, ct);
            
            _pendingTasks.Add(SaveStamps(stampsArr, ser, ct));
            
            return result;
        }
        
        public async ValueTask<Result<bool>> Commit(
            IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct) {
            var premisesArr = premises as EntityStampKey[] ?? premises.ToArray();
            if (premisesArr.Length == 0) return new Result<bool>(false);

            var commitResult = await _transaction.Commit(premisesArr, conclusions, ct);

            if (commitResult.Error is Error.TransactionError err) {
                var piSecArr = err.Conflicts
                    .Where(x => CognitionId.IsPisec(x.PremiseCid, x.ActualCid)).ToArray();
                if (piSecArr.Length > 0) {
                    _pendingTasks.Add(CheckCollisions(piSecArr, null!, ct));
                }
            }

            return commitResult;
        }
        
        public Task Flush() {
            var tasks = _pendingTasks;
            _pendingTasks = new ConcurrentBag<Task>();
            return Task.WhenAll(tasks);
        }

        private async Task<Result<EntityStamp>> LoadActualStamp(
            EntityId eid, CognitionId before, ISerializer ser, CancellationToken ct) {
            while (true) {
                ct.ThrowIfCancellationRequested();
                
                var historyResult = await _repo.GetHistory(eid, before, 42);
                if (historyResult.IsError) return new Result<EntityStamp>(historyResult.Error);

                var cid = historyResult.Value.FirstOrDefault(x => x.IsTruth());
                if (!cid.IsNull()) {
                    return await _repo.Get(new EntityStampKey(eid, cid), ser);
                }

                before = historyResult.Value.LastOrDefault();
                if (before.IsNull()) return new Result<EntityStamp>(Error.NotFound);
            }
        }

        private Task SaveStamps(IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct) =>
            Task.WhenAll(stamps.Select(x => _repo.Put(x, ser).AsTask()));

        private Task CheckCollisions(IEnumerable<Error.TransactionError.Conflict> conflicts, ISerializer ser, CancellationToken ct) =>
            Task.WhenAll(conflicts.Select(x => CheckCollision(x, ser, ct)));
        
        private async Task CheckCollision(Error.TransactionError.Conflict conflict, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var cacheGetResult = await _cache.Get(conflict.Eid, ser, ct);

            if (cacheGetResult.IsOk && cacheGetResult.Value.IsOk && cacheGetResult.Value.Cid == conflict.ActualCid) {
                // cache == actualCid, all ok
                return;
            }

            var repoResult = await LoadActualStamp(conflict.Eid, CognitionId.Singularity, ser, ct);

            if (repoResult.IsError) {
                // log error
                return;
            }

            var commitResult = await _transaction.Commit(
                new[] {new EntityStampKey(conflict.Eid, conflict.ActualCid)}, 
                new[] {new EntityStampKey(conflict.Eid, repoResult.Value.Cid)}, 
                ct);

            if (commitResult.IsOk) {
                if (commitResult.Value) {
                    var cacheSetResult = await _cache.Set(new[] {repoResult.Value.ToEntry()}, ser, ct);
                    if (cacheSetResult.IsError) {
                        // log error
                    }
                }
            }
            else {
                // log error
            }
        }
    }
}