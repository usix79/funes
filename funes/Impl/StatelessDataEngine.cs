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
        private readonly ConcurrentQueue<Task> _tasksQueue = new ();

        public StatelessDataEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger) =>
            (_logger, _repo, _cache, _tre) = (logger, repo, cache, tre);

        public async ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer ser, CancellationToken ct) {
            var cacheResult = await _cache.Get(eid, ser, ct);

            if (cacheResult.IsOk) return cacheResult;

            var ss = new StreamSerializer();
        
            // cache miss, look in the repo
            if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug($"Retrieve {eid}, cache miss");
            
            var repoResult = await LoadActualStamp(eid, IncrementId.Singularity, ss, ct);

            if (repoResult.IsError && repoResult.Error != Error.NotFound)
                return new Result<EntityEntry>(repoResult.Error);

            var entry = repoResult.IsOk 
                ? repoResult.Value.ToEntry() 
                : EntityEntry.NotExist(eid);
            
            // try set cache item
            var trySetResult = await _cache.UpdateIfNewer(new[] {entry}, ss, ct);

            if (trySetResult.IsOk && !trySetResult.Value) {
                // what? cache already has gotten a value, look in cache again
                if (_logger.IsEnabled(LogLevel.Debug)) 
                    _logger.LogDebug($"Retrieve {eid}, unsuccessfull cache update");
                
                return await _cache.Get(eid, ser, ct);
            }

            if (trySetResult.IsError)
                _logger.LogError($"Retrieve {eid}, cache update error {trySetResult.Error}");

            if (entry.IsOk) {
                var realDecodeResult = await ss.DecodeForReal(eid, ser);
                if (realDecodeResult.IsError) return new Result<EntityEntry>(realDecodeResult.Error);
                entry = entry.MapValue(realDecodeResult.Value);
            }

            return new Result<EntityEntry>(entry);
        }
        
        public async ValueTask<Result<bool>> Upload(
            IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct, bool skipCache = false) {

            var result = true;
            if (skipCache) {
                _tasksQueue.Enqueue(SaveStamps(stamps, ser, ct));
                result = false;
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
                    var cacheResult = await _cache.UpdateIfNewer(stampsList.Select(x => x.ToEntry()), ss, ct);
                    if (cacheResult.IsError) {
                        errors ??= new ();
                        errors.Add(cacheResult.Error);
                    }
                    result = cacheResult.Value;
                    _tasksQueue.Enqueue(SaveStamps(stampsList, ss, ct));
                }

                if (errors?.Count > 0) return Result<bool>.AggregateError(errors); 
            }
            return new Result<bool>(result);
        }
        
        public async ValueTask<Result<Void>> TryCommit(IEnumerable<EntityStampKey> premises, 
                IEnumerable<EntityStampKey> conclusions, CancellationToken ct) {

            var commitResult = await _tre.TryCommit(premises, conclusions, ct);

            if (commitResult.Error is Error.CommitError err) {
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

        private bool IsPiSec(Error.CommitError.Conflict conflict) { 
            if (conflict.ActualIncId.IsOlderThan(conflict.PremiseIncId)) return true;
            
            // if actualIncId is OlderThan 3.14sec
            if (conflict.ActualIncId.IsOlderThan(
                IncrementId.ComposeId(DateTimeOffset.UtcNow.AddMilliseconds(-3140), ""))) return true;
            
            return false;
        }

        private async Task<Result<EntityStamp>> LoadActualStamp(
            EntityId eid, IncrementId before, ISerializer ser, CancellationToken ct) {
            while (true) {
                ct.ThrowIfCancellationRequested();
                
                var historyResult = await _repo.History(eid, before, 42, ct);
                if (historyResult.IsError) return new Result<EntityStamp>(historyResult.Error);

                var incId = historyResult.Value.FirstOrDefault(x => x.IsSuccess());
                if (!incId.IsNull()) {
                    return await _repo.Load(new EntityStampKey(eid, incId), ser, ct);
                }

                before = historyResult.Value.LastOrDefault();
                if (before.IsNull()) return new Result<EntityStamp>(Error.NotFound);
            }
        }

        private Task SaveStamps(IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct) =>
            Task.WhenAll(stamps.Select(x => _repo.Save(x, ser, ct).AsTask()));

        private Task CheckCollisions(IEnumerable<Error.CommitError.Conflict> conflicts, CancellationToken ct) =>
            Task.WhenAll(conflicts.Select(x => CheckConflict(x, ct)));
        
        private async Task CheckConflict(Error.CommitError.Conflict conflict, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var ss = new StreamSerializer();
            var cacheGetResult = await _cache.Get(conflict.EntId, ss, ct);

            if (cacheGetResult.IsOk && cacheGetResult.Value.IsOk && cacheGetResult.Value.IncId == conflict.ActualIncId) {
                // cache == actualIncId, all ok
                return;
            }

            _logger.LogError($"PiSec confirmed in {nameof(StatelessDataEngine)}, {conflict}, stamp in cache {cacheGetResult.Value.IncId}");
            var repoResult = await LoadActualStamp(conflict.EntId, IncrementId.Singularity, ss, ct);
            if (repoResult.IsError) {
                _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, LoadActualStamp error {repoResult.Error}");
                return;
            }

            var commitResult = await _tre.TryCommit(
                new[] {conflict.EntId.CreateStampKey(conflict.ActualIncId)}, 
                new[] {conflict.EntId.CreateStampKey(repoResult.Value.IncId)}, 
                ct);

            if (commitResult.IsOk) {
                var cacheSetResult = await _cache.Set(repoResult.Value.ToEntry(), ss, ct);
                if (cacheSetResult.IsError) {
                    _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, Cache Set error {cacheSetResult.Error}");
                }
                else {
                    _logger.LogWarning($"PiSec resolved in {nameof(StatelessDataEngine)}, {conflict}, with {repoResult.Value.IncId}");
                }
            }
            else {
                _logger.LogError($"{nameof(StatelessDataEngine)} Unable to resolve piSec, TryCommit error {commitResult.Error}");
            }
        }
    }
}