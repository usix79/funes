using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes.Impl {
    public class StatelessDataEngine: IDataEngine {
        private readonly ILogger _logger;
        private readonly IRepository _repo;
        private readonly ICache _cache;
        private readonly ITransactionEngine _tre;
        private ConcurrentQueue<Task> _tasksQueue = new ();

        public StatelessDataEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger) =>
            (_logger, _repo, _cache, _tre) = (logger, repo, cache, tre);

        public async ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer ser, CancellationToken ct) {
            var cacheResult = await _cache.Get(eid, ser, ct);

            if (cacheResult.IsOk) return cacheResult;

            var ss = new StreamSerializer();
        
            // cache miss, look in the repo
            if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug($"Retrieve {eid}, cache miss");
            
            var repoResult = await LoadActualStamp(eid, CognitionId.Singularity, ss, ct);

            if (repoResult.IsError && repoResult.Error != Error.NotFound)
                return new Result<EntityEntry>(repoResult.Error);

            var entry = repoResult.IsOk 
                ? repoResult.Value.ToEntry() 
                : EntityEntry.NotExist;
            
            // try set cache item
            var trySetResult = await _cache.UpdateIfOlder(new[] {entry}, ss, ct);

            if (trySetResult.IsOk && !trySetResult.Value) {
                // what? cache already has gotten a value, look in cache again
                if (_logger.IsEnabled(LogLevel.Debug)) 
                    _logger.LogDebug($"Retrieve {eid}, unsuccessfull cache update");
                
                return await _cache.Get(eid, ser, ct);
            }

            if (trySetResult.IsError)
                _logger.LogError($"Retrieve {eid}, cache update error {trySetResult.Error}");

            var realDecodeResult = await ss.DecodeForReal(eid, ser);
            if (repoResult.IsError) return new Result<EntityEntry>(repoResult.Error);

            return new Result<EntityEntry>(entry.MapValue(realDecodeResult.Value));
        }
        
        public async ValueTask<Result<bool>> Upload(
            IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct, bool skipCache = false) {

            if (skipCache) {
                _tasksQueue.Enqueue(SaveStamps(stamps, ser, ct));
            }
            else {
                var stampsList = new List<EntityStamp>();
                List<Error>? errors = null; 
                var ss = new StreamSerializer();
            
                foreach (var stamp in stamps) {
                    var encodeResult = await ss.EncodeForReal(stamp.Entity.Id, stamp.Entity.Value, ser);
                    if (encodeResult.IsOk) {
                        stampsList.Add(stamp);
                    }
                    else{
                        errors ??= new ();
                        errors.Add(encodeResult.Error);
                    }
                }

                if (stampsList.Count > 0) {
                    var cacheResult = await _cache.UpdateIfOlder(stampsList.Select(x => x.ToEntry()), ss, ct);
                    if (cacheResult.IsError) {
                        errors ??= new ();
                        errors.Add(cacheResult.Error);
                    }
                    _tasksQueue.Enqueue(SaveStamps(stampsList, ss, ct));
                }

                if (errors?.Count > 0) return Result<bool>.AggregateError(errors); 
            }
            return new Result<bool>(true);
        }
        
        public async ValueTask<Result<bool>> Commit(IEnumerable<EntityStampKey> premises, 
                IEnumerable<EntityStampKey> conclusions, CancellationToken ct) {
            var premisesArr = premises as EntityStampKey[] ?? premises.ToArray();
            if (premisesArr.Length == 0) return new Result<bool>(false);

            var commitResult = await _tre.Commit(premisesArr, conclusions, ct);

            if (commitResult.Error is Error.TransactionError err) {
                var piSecArr = err.Conflicts.Where(IsPiSec).ToArray();
                if (piSecArr.Length > 0) {
                    var conflictsTxt = string.Join(',', piSecArr.Select(x => x.ToString()));
                    _logger.LogWarning($"Possible piSec in {nameof(StatelessDataEngine)}, {conflictsTxt}");
                    _tasksQueue.Enqueue(CheckCollisions(piSecArr, ct));
                }
            }

            return commitResult;
        }
        
        public Task Flush() {
            if (_tasksQueue.IsEmpty) return Task.CompletedTask;

            var tasks = new List<Task>(_tasksQueue.Count);
            while(_tasksQueue.TryDequeue(out var task)) tasks.Add(task);

            return Task.WhenAll(tasks);
        }

        private bool IsPiSec(Error.TransactionError.Conflict conflict) { 
            if (conflict.PremiseCid.IsOlderThan(conflict.ActualCid)) return true;
            
            // if actualCid is OlderThan 3.14sec
            if (conflict.PremiseCid.IsOlderThan(
                CognitionId.ComposeId(DateTimeOffset.UtcNow.AddMilliseconds(-3140), ""))) return true;
            
            return false;
        }

        private async Task<Result<EntityStamp>> LoadActualStamp(
            EntityId eid, CognitionId before, ISerializer ser, CancellationToken ct) {
            while (true) {
                ct.ThrowIfCancellationRequested();
                
                var historyResult = await _repo.History(eid, before, 42);
                if (historyResult.IsError) return new Result<EntityStamp>(historyResult.Error);

                var cid = historyResult.Value.FirstOrDefault(x => x.IsTruth());
                if (!cid.IsNull()) {
                    return await _repo.Load(new EntityStampKey(eid, cid), ser, ct);
                }

                before = historyResult.Value.LastOrDefault();
                if (before.IsNull()) return new Result<EntityStamp>(Error.NotFound);
            }
        }

        private Task SaveStamps(IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct) =>
            Task.WhenAll(stamps.Select(x => _repo.Save(x, ser, ct).AsTask()));

        private Task CheckCollisions(IEnumerable<Error.TransactionError.Conflict> conflicts, CancellationToken ct) =>
            Task.WhenAll(conflicts.Select(x => CheckConflict(x, ct)));
        
        private async Task CheckConflict(Error.TransactionError.Conflict conflict, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var ss = new StreamSerializer();
            var cacheGetResult = await _cache.Get(conflict.Eid, ss, ct);

            if (cacheGetResult.IsOk && cacheGetResult.Value.IsOk && cacheGetResult.Value.Cid == conflict.ActualCid) {
                // cache == actualCid, all ok
                return;
            }

            _logger.LogError($"PiSec confirmed in {nameof(StatelessDataEngine)}, {conflict}, stamp in cache {cacheGetResult.Value.Cid}");
            var repoResult = await LoadActualStamp(conflict.Eid, CognitionId.Singularity, ss, ct);

            if (repoResult.IsError) {
                _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, LoadActualStamp error {repoResult.Error}");
                return;
            }

            var commitResult = await _tre.Commit(
                new[] {conflict.Eid.CreateStampKey(conflict.ActualCid)}, 
                new[] {conflict.Eid.CreateStampKey(repoResult.Value.Cid)}, 
                ct);

            if (commitResult.IsOk) {
                if (commitResult.Value) {
                    var cacheSetResult = await _cache.Set(repoResult.Value.ToEntry(), ss, ct);
                    if (cacheSetResult.IsError) {
                        _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, Cache Set error {cacheSetResult.Error}");
                    }
                }
            }
            else {
                _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, Commit error {commitResult.Error}");
            }
        }
    }
}