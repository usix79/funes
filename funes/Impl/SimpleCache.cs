using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    public class SimpleCache : ICache {
        
        private struct EventLogEntry {
            public ReadOnlyMemory<byte> Data { get; init; }
            public List<(IncrementId, int)> Records { get; init; }
        }
        
        private readonly ConcurrentDictionary<EntityId, BinaryStamp> _stamps = new();
        private readonly Dictionary<EntityId, EventLogEntry> _events = new();
        
        public Task<Result<BinaryStamp>> Get(EntityId eid, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_stamps.TryGetValue(eid, out var stamp))
                return Task.FromResult(Result<BinaryStamp>.NotFound);
            
            return Task.FromResult(new Result<BinaryStamp>(stamp));
        }

        public Task<Result<Void>> Set(BinaryStamp stamp, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            _stamps[stamp.Eid] = stamp;
            return Task.FromResult(new Result<Void>(Void.Value));
        }

        public Task<Result<Void>> UpdateIfNewer(BinaryStamp stamp, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            
            if (_stamps.TryGetValue(stamp.Eid, out var currentStamp)) {
                if (!stamp.IncId.IsNewerThan(currentStamp.IncId)){
                    return Task.FromResult(new Result<Void>(Void.Value));
                }
            }
            
            _stamps[stamp.Eid] = stamp;
            
            return Task.FromResult(new Result<Void>(Void.Value));
        }

        public Task<Result<Void>> UpdateEventsIfNotExists(EntityId eid, Event[] events, CancellationToken ct) {
            lock (_events) {
                if (!_events.ContainsKey(eid)) {

                    var memory = new Memory<byte>(new byte[events.Sum(x => x.Data.Length)]);
                    var entry = new EventLogEntry {Data = memory, Records = new List<(IncrementId, int)>()};
                    var idx = 0;
                    foreach (var evt in events) {
                        evt.Data.CopyTo(memory.Slice(idx));
                        entry.Records.Add((evt.IncId, evt.Data.Length));
                        idx += evt.Data.Length;
                    }

                    _events.Add(eid, entry);
                }
            }
            return Task.FromResult(new Result<Void>(Void.Value));
        }

        public Task<Result<int>> AppendEvent(EntityId eid, Event evt, CancellationToken ct) {
            lock (_events) {
                if (!_events.TryGetValue(eid, out var entry)) 
                    return Task.FromResult(new Result<int>(Error.NotFound));

                var memory = new Memory<byte>(new byte[entry.Data.Length + evt.Data.Length]);
                entry.Data.CopyTo(memory);
                evt.Data.CopyTo(memory.Slice(entry.Data.Length));
                entry.Records.Add((evt.IncId, evt.Data.Length));
                var newEntry = new EventLogEntry {Data = memory, Records = entry.Records};
                _events[eid] = newEntry;

                return Task.FromResult(new Result<int>(newEntry.Records.Count));
            }
        }

        public Task<Result<EventLog>> GetEventLog(EntityId eid, CancellationToken ct) {
            lock (_events) {
                if (!_events.TryGetValue(eid, out var entry))
                    return Task.FromResult(new Result<EventLog>(Error.NotFound));

                var eventLog = entry.Records.Count > 0
                    ? new EventLog(entry.Records[0].Item1, entry.Records[^1].Item1, entry.Data)
                    : new EventLog(IncrementId.None, IncrementId.None, entry.Data);

                return Task.FromResult(new Result<EventLog>(eventLog));
            }
        }

        public Task<Result<Void>> TruncateEvents(EntityId eid, IncrementId since, CancellationToken ct) {
            lock (_events) {
                if (!_events.TryGetValue(eid, out var entry))
                    return Task.FromResult(new Result<Void>(Error.NotFound));

                var found = false;
                var countToTruncate = 0;
                var sizeToTruncate = 0;
                foreach (var (incId, size) in entry.Records) {
                    countToTruncate++;
                    sizeToTruncate += size;
                    if (incId == since) {
                        found = true;
                        break;
                    }
                }
                
                if (!found) return Task.FromResult(new Result<Void>(Error.NotFound));

                entry.Records.RemoveRange(0, countToTruncate);
                var newEntry = new EventLogEntry {
                    Data = entry.Data.Slice(sizeToTruncate), 
                    Records = entry.Records
                };

                _events[eid] = newEntry;
                
                return Task.FromResult(new Result<Void>(Void.Value));
            }
        }
    }
}