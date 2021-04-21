using System;
using System.Buffers;
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
        private readonly ConcurrentQueue<Task> _backgroundTasks = new ();
        
        public StatelessDataEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger) =>
            (_logger, _repo, _cache, _tre) = (logger, repo, cache, tre);

        public async ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct) {
            var cacheResult = await _cache.Get(eid, ct);

            if (cacheResult.IsOk) return cacheResult;

            // cache miss, look in the repo
            _logger.FunesDebug(nameof(StatelessDataEngine),"Cache miss", eid.Id);
        
            var repoResult = await LoadActualStamp(eid, IncrementId.Singularity, ct);

            if (repoResult.IsError && repoResult.Error != Error.NotFound)
                return repoResult;

            var stamp = repoResult.IsOk ? repoResult.Value : BinaryStamp.Empty(eid);
        
            // try set cache item
            var trySetResult = await _cache.UpdateIfNewer(stamp, ct);

            if (trySetResult.IsError)
                _logger.FunesError(nameof(StatelessDataEngine), "_cache.UpdateIfNewer", trySetResult.Error);
            
            return new Result<BinaryStamp>(stamp);
        }
        
        public async ValueTask<Result<Void>> Upload(BinaryStamp stamp, CancellationToken ct, bool skipCache = false) {

            _backgroundTasks.Enqueue(_repo.Save(stamp, ct));

            return skipCache
                ? new Result<Void>(Void.Value)
                : await _cache.UpdateIfNewer(stamp, ct);
        }
        
        public async ValueTask<Result<Void>> TryCommit(ArraySegment<StampKey> premises,
            ArraySegment<EntityId> outputs, IncrementId incId, CancellationToken ct) {

            var commitResult = await _tre.TryCommit(premises, outputs, incId, ct);

            if (commitResult.Error is Error.CommitError err) {
                var piSecArr = err.Conflicts.Where(IsPiSec).ToArray();
                if (piSecArr.Length > 0) {
                    var conflictsTxt = string.Join(',', piSecArr.Select(x => x.ToString()));
                    _logger.FunesWarning(nameof(StatelessDataEngine), "Possible piSec", conflictsTxt);
                    _backgroundTasks.Enqueue(CheckCollisions(piSecArr, ct));
                }
            }

            return commitResult;
        }
        
        public async ValueTask<Result<int>> AppendEvent(EntityId recordsId, Event evt, EntityId offsetId, 
            CancellationToken ct, bool skipCache = false) {
            if (skipCache) {
                _backgroundTasks.Enqueue(SaveEvent(recordsId, evt, ct));
                return new Result<int>(0);
            }

            var appendResult = await _cache.AppendEvent(recordsId, evt, ct);
            if (appendResult.Error == Error.NotFound) {
                var updateResult = await UpdateEventLogInCache(recordsId, offsetId, ct);
                if (updateResult.IsError) return new Result<int>(updateResult.Error);

                // snd try
                appendResult = await _cache.AppendEvent(recordsId, evt, ct);
            }
            
            _backgroundTasks.Enqueue(SaveEvent(recordsId, evt, ct));
            return appendResult;
        }

        public async ValueTask<Result<EventLog>> RetrieveEventLog(
            EntityId recordsId, EntityId offsetId, CancellationToken ct) {

            var getEventLogResult = await _cache.GetEventLog(recordsId, ct);

            if (getEventLogResult.Error == Error.NotFound) {
                var updateResult = await UpdateEventLogInCache(recordsId, offsetId, ct);
                if (updateResult.IsError) return new Result<EventLog>(updateResult.Error);
                getEventLogResult = await _cache.GetEventLog(recordsId, ct);
            }

            return getEventLogResult;
        }

        private async ValueTask<Result<Void>> UpdateEventLogInCache(
            EntityId eventLogId, EntityId offsetId, CancellationToken ct) {
            
            var offsetResult = await Retrieve(offsetId, ct);
            if (offsetResult.IsError) return new Result<Void>(offsetResult.Error);

            var after = IncrementId.BigBang;
            if (offsetResult.Value.IsNotEmpty) {
                var offset = new EventOffset(offsetResult.Value.Data);

                after = offset.GetLastIncId();
            }

            var historyResult = await _repo.HistoryAfter(eventLogId, after, ct);
            if (historyResult.IsError) return new Result<Void>(historyResult.Error);
            var arr = new Event[historyResult.Value.Length];
            for(var i = 0; i < arr.Length; i++) {
                var incId = historyResult.Value[i];
                var loadResult = await _repo.Load(eventLogId.CreateStampKey(incId), ct);
                if (loadResult.IsError) return new Result<Void>(loadResult.Error);
                arr[i] = new Event(incId, loadResult.Value.Data.Memory);
            }
                
            var updateResult = await _cache.UpdateEventsIfNotExists(eventLogId, arr, ct);
            if (updateResult.IsError) return new Result<Void>(updateResult.Error);

            return new Result<Void>(Void.Value);
        }

        public async ValueTask<Result<Void>> TruncateEvents(
            EntityId recordId, IncrementId lastToTruncate, CancellationToken ct) {

            return await _cache.TruncateEvents(recordId, lastToTruncate, ct);
        }        

        public async ValueTask Flush(CancellationToken ct = default) {
            if (_backgroundTasks.IsEmpty) return;

            var tasksArr = ArrayPool<Task>.Shared.Rent(_backgroundTasks.Count);
            try {
                var idx = 0;
                while(_backgroundTasks.TryDequeue(out var task) && idx < tasksArr.Length) tasksArr[idx++] = task;
                await Utils.Tasks.WhenAll(new ArraySegment<Task>(tasksArr, 0, idx), ct);
            }
            finally {
                ArrayPool<Task>.Shared.Return(tasksArr);
            }
        }

        private bool IsPiSec(Error.CommitError.Conflict conflict) { 
            if (conflict.ActualIncId.IsOlderThan(conflict.PremiseIncId)) return true;
            
            // if actualIncId is OlderThan 3.14sec
            if (conflict.ActualIncId.IsOlderThan(
                IncrementId.ComposeId(DateTimeOffset.UtcNow.AddMilliseconds(-3140), ""))) return true;
            
            return false;
        }

        private async Task<Result<BinaryStamp>> LoadActualStamp(
            EntityId eid, IncrementId before, CancellationToken ct) {
            while (true) {
                ct.ThrowIfCancellationRequested();
                
                var historyResult = await _repo.HistoryBefore(eid, before, 42, ct);
                if (historyResult.IsError) return new Result<BinaryStamp>(historyResult.Error);

                var incId = historyResult.Value.FirstOrDefault(x => x.IsSuccess());
                if (!incId.IsNull) {
                    return await _repo.Load(new StampKey(eid, incId), ct);
                }

                before = historyResult.Value.LastOrDefault();
                if (before.IsNull) return new Result<BinaryStamp>(Error.NotFound);
            }
        }

        private Task SaveEvent(EntityId entId, Event evt, CancellationToken ct) {
            var stamp = new BinaryStamp(entId.CreateStampKey(evt.IncId), new BinaryData("evt", evt.Data)); 
            return _repo.Save(stamp, ct);
        }

        private Task CheckCollisions(IEnumerable<Error.CommitError.Conflict> conflicts, CancellationToken ct) =>
            Task.WhenAll(conflicts.Select(x => CheckConflict(x, ct)));
        
        private async Task CheckConflict(Error.CommitError.Conflict conflict, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            var cacheGetResult = await _cache.Get(conflict.EntId, ct);

            if (cacheGetResult.IsOk && cacheGetResult.Value.IncId == conflict.ActualIncId) {
                // cache == actualIncId, all ok
                return;
            }

            _logger.FunesError(nameof(StatelessDataEngine), "PiSec", cacheGetResult.Value.IncId, conflict.ToString());
            
            var repoResult = await LoadActualStamp(conflict.EntId, IncrementId.Singularity, ct);
            if (repoResult.IsError) {
                _logger.FunesError(nameof(StatelessDataEngine), "LoadActualStamp", repoResult.Error);
                return;
            }

            var commitResult = await _tre.TryCommit(
                new[] {conflict.EntId.CreateStampKey(conflict.ActualIncId)}, 
                new[] {conflict.EntId}, repoResult.Value.IncId,
                ct);

            if (commitResult.IsOk) {
                var cacheSetResult = await _cache.Set(repoResult.Value, ct);
                if (cacheSetResult.IsError) {
                    _logger.FunesError(nameof(StatelessDataEngine), "_cache.Set", cacheGetResult.Error);
                }
                else {
                    _logger.FunesWarning(nameof(StatelessDataEngine), "PiSec resolved", repoResult.Value.IncId);
                }
            }
            else {
                _logger.FunesError(nameof(StatelessDataEngine), "TryCommit", commitResult.Error);
            }
        }
    }
}