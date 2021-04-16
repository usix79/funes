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

        private readonly ObjectPool<StreamSerializer> _ssPool =
            new (() => new StreamSerializer(), 21);

        public StatelessDataEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger) =>
            (_logger, _repo, _cache, _tre) = (logger, repo, cache, tre);

        public async ValueTask<Result<EntityEntry>> Retrieve(EntityId eid, ISerializer ser, CancellationToken ct) {
            var cacheResult = await _cache.Get(eid, ser, ct);

            if (cacheResult.IsOk) return cacheResult;

            var ss = _ssPool.Rent()!;

            try {
                // cache miss, look in the repo
                if (_logger.IsEnabled(LogLevel.Debug)) _logger.LogDebug($"Retrieve {eid}, cache miss");
            
                var repoResult = await LoadActualStamp(eid, IncrementId.Singularity, ss, ct);

                if (repoResult.IsError && repoResult.Error != Error.NotFound)
                    return new Result<EntityEntry>(repoResult.Error);

                var entry = repoResult.IsOk 
                    ? repoResult.Value.ToEntry() 
                    : EntityEntry.NotExist(eid);
            
                // try set cache item
                var trySetResult = await _cache.UpdateIfNewer(entry, ss, ct);

                if (trySetResult.IsError)
                    _logger.LogError($"Retrieve {eid}, cache update error {trySetResult.Error}");

                if (entry.IsOk) {
                    var realDecodeResult = await ss.DecodeForReal(eid, ser, ct);
                    if (realDecodeResult.IsError) return new Result<EntityEntry>(realDecodeResult.Error);
                    entry = entry.MapValue(realDecodeResult.Value);
                }

                return new Result<EntityEntry>(entry);
            }
            finally{
                ss.Reset();
                _ssPool.Return(ss);
            }
        }
        
        public async ValueTask<Result<Void>> Upload(EntityStamp stamp, 
            ISerializer ser, CancellationToken ct, bool skipCache = false) {

            if (skipCache) {
                _tasksQueue.Enqueue(SaveStamp(stamp, ser, ct));
                return new Result<Void>(Void.Value);
            }
            
            var ss = _ssPool.Rent()!;
            try {
                var encodeResult = await ss.EncodeForReal(stamp.Entity.Id, stamp.Entity.Value, ser, ct);
                if (encodeResult.IsError) return new Result<Void>(encodeResult.Error);
            
                var cacheResult = await _cache.UpdateIfNewer(stamp.ToEntry(), ss, ct);
                _tasksQueue.Enqueue(SaveStamp(stamp, ss, ct));

                if (cacheResult.IsError) return new Result<Void>(cacheResult.Error);
            }
            finally {
                ss.Reset();
                _ssPool.Return(ss);
            }
            
            return new Result<Void>(Void.Value);
        }
        
        public async ValueTask<Result<Void>> TryCommit(ArraySegment<EntityStampKey> premises,
            ArraySegment<EntityId> outputs, IncrementId incId, CancellationToken ct) {

            var commitResult = await _tre.TryCommit(premises, outputs, incId, ct);

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
        
        public async ValueTask<Result<int>> AppendEvent(EntityId recordsId, Event evt, EntityId offsetId, 
            CancellationToken ct, bool skipCache = false) {
            if (skipCache) {
                _tasksQueue.Enqueue(SaveEvent(recordsId, evt, ct));
                return new Result<int>(0);
            }

            var appendResult = await _cache.AppendEvent(recordsId, evt, ct);
            if (appendResult.Error == Error.NotFound) {
                var updateResult = await UpdateEventLogInCache(recordsId, offsetId, ct);
                if (updateResult.IsError) return new Result<int>(updateResult.Error);

                // snd try
                appendResult = await _cache.AppendEvent(recordsId, evt, ct);
            }
            
            _tasksQueue.Enqueue(SaveEvent(recordsId, evt, ct));
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

            return getEventLogResult.IsOk
                ? new Result<EventLog>(getEventLogResult.Value)
                : new Result<EventLog>(getEventLogResult.Error);
        }

        private async ValueTask<Result<Void>> UpdateEventLogInCache(
            EntityId eventLogId, EntityId offsetId, CancellationToken ct) {
            
            var offsetResult = await Retrieve(offsetId, StringSerializer.Instance, ct);
            if (offsetResult.IsError) return new Result<Void>(offsetResult.Error);
            var after = offsetResult.Value.IsOk 
                ?  new IncrementId((string) offsetResult.Value.Value)
                : IncrementId.BigBang;
                
            var historyResult = await _repo.HistoryAfter(eventLogId, after, ct);
            if (historyResult.IsError) return new Result<Void>(historyResult.Error);
            var arr = new Event[historyResult.Value.Length];
            for(var i = 0; i < arr.Length; i++) {
                var incId = historyResult.Value[i];
                var loadResult = await _repo.LoadBinary(eventLogId.CreateStampKey(incId), ct);
                if (loadResult.IsError) return new Result<Void>(loadResult.Error);
                arr[i] = new Event(incId, loadResult.Value);
            }
                
            var updateResult = await _cache.UpdateEventsIfNotExists(eventLogId, arr, ct);
            if (updateResult.IsError) return new Result<Void>(updateResult.Error);

            return new Result<Void>(Void.Value);
        }

        public async ValueTask<Result<Void>> TruncateEvents(
            EntityId recordId, EntityStampKey offsetKey, IncrementId lastToTruncate, CancellationToken ct) {

            var offsetStamp = new EntityStamp(offsetKey, lastToTruncate.Id);
            var uploadOffsetResult = await Upload(offsetStamp, StringSerializer.Instance, ct);
            if (uploadOffsetResult.IsError) return new Result<Void>(uploadOffsetResult.Error);

            return await _cache.TruncateEvents(recordId, lastToTruncate, ct);
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
                
                var historyResult = await _repo.HistoryBefore(eid, before, 42, ct);
                if (historyResult.IsError) return new Result<EntityStamp>(historyResult.Error);

                var incId = historyResult.Value.FirstOrDefault(x => x.IsSuccess());
                if (!incId.IsNull) {
                    return await _repo.Load(new EntityStampKey(eid, incId), ser, ct);
                }

                before = historyResult.Value.LastOrDefault();
                if (before.IsNull) return new Result<EntityStamp>(Error.NotFound);
            }
        }

        private Task SaveStamp(EntityStamp stamp, ISerializer ser, CancellationToken ct) =>
            _repo.Save(stamp, ser, ct).AsTask();

        private Task SaveEvent(EntityId entId, Event evt, CancellationToken ct) =>
            _repo.SaveBinary(entId.CreateStampKey(evt.IncId), evt.Data, ct).AsTask();

        private Task CheckCollisions(IEnumerable<Error.CommitError.Conflict> conflicts, CancellationToken ct) =>
            Task.WhenAll(conflicts.Select(x => CheckConflict(x, ct)));
        
        private async Task CheckConflict(Error.CommitError.Conflict conflict, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            var ss = _ssPool.Rent()!;

            try {
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
                    new[] {conflict.EntId}, repoResult.Value.IncId,
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
            finally {
                ss.Reset();
                _ssPool.Return(ss);
            }
        }
    }
}