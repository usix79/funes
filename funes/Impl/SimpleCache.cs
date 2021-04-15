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
        
        private readonly ConcurrentDictionary<EntityId, (IncrementId, MemoryStream?, string)> _entitiesData = new();
        private readonly Dictionary<EntityId, EventLogEntry> _eventLogs = new();
        
        public async Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            if (!_entitiesData.TryGetValue(eid, out var triple)) return Result<EntityEntry>.NotFound;

            if (triple.Item2 == null) return new Result<EntityEntry>(EntityEntry.NotExist(eid));
            
            triple.Item2.Position = 0;
            var serResult = await ser.Decode(triple.Item2, eid, triple.Item3);
            if (serResult.IsError) return new Result<EntityEntry>(serResult.Error);

            return new Result<EntityEntry>(EntityEntry.Ok(new Entity(eid, serResult.Value), triple.Item1));
        }

        public async Task<Result<Void>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            MemoryStream? stream = null;
            string encoding = "";

            if (entry.IsOk) {
                stream = new MemoryStream();
                var serResult = await ser.Encode(stream, entry.EntId, entry.Value);
                if (serResult.IsError) return new Result<Void>(serResult.Error);
                encoding = serResult.Value;
            }

            _entitiesData[entry.EntId] = (entry.IncId, stream, encoding);
            return new Result<Void>(Void.Value);
        }

        public async Task<Result<Void>> UpdateIfNewer(EntityEntry entry, ISerializer ser, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();
            
            if (_entitiesData.TryGetValue(entry.EntId, out var triple)) {
                if (!entry.IncId.IsNewerThan(triple.Item1)){
                    return new Result<Void>(Void.Value);
                }
            }
            
            (MemoryStream? stream, string encoding) = (null, "");
            if (entry.IsOk) {
                stream = new MemoryStream();
                var serResult = await ser.Encode(stream, entry.EntId, entry.Value);
                if (serResult.IsError) return new Result<Void>(serResult.Error);
                encoding = serResult.Value;
            }

            _entitiesData[entry.EntId] = (entry.IncId, stream, encoding);
            
            return new Result<Void>(Void.Value);
        }

        public Task<Result<Void>> UpdateEventsIfNotExists(EntityId eid, Event[] events, CancellationToken ct) {
            lock (_eventLogs) {
                if (!_eventLogs.ContainsKey(eid)) {

                    var memory = new Memory<byte>(new byte[events.Sum(x => x.Data.Length)]);
                    var entry = new EventLogEntry {Data = memory, Records = new List<(IncrementId, int)>()};
                    var idx = 0;
                    foreach (var evt in events) {
                        evt.Data.CopyTo(memory.Slice(idx));
                        entry.Records.Add((evt.IncId, evt.Data.Length));
                        idx += evt.Data.Length;
                    }

                    _eventLogs.Add(eid, entry);
                }
            }
            return Task.FromResult(new Result<Void>(Void.Value));
        }

        public Task<Result<int>> AppendEvent(EntityId eid, Event evt, CancellationToken ct) {
            lock (_eventLogs) {
                if (!_eventLogs.TryGetValue(eid, out var entry)) 
                    return Task.FromResult(new Result<int>(Error.NotFound));

                var memory = new Memory<byte>(new byte[entry.Data.Length + evt.Data.Length]);
                entry.Data.CopyTo(memory);
                evt.Data.CopyTo(memory.Slice(entry.Data.Length));
                entry.Records.Add((evt.IncId, evt.Data.Length));
                var newEntry = new EventLogEntry {Data = memory, Records = entry.Records};
                _eventLogs[eid] = newEntry;

                return Task.FromResult(new Result<int>(newEntry.Records.Count));
            }
        }

        public Task<Result<EventLog>> GetEventLog(EntityId eid, CancellationToken ct) {
            lock (_eventLogs) {
                if (!_eventLogs.TryGetValue(eid, out var entry))
                    return Task.FromResult(new Result<EventLog>(Error.NotFound));

                var eventLog = entry.Records.Count > 0
                    ? new EventLog(entry.Records[0].Item1, entry.Records[^1].Item1, entry.Data)
                    : new EventLog(IncrementId.None, IncrementId.None, entry.Data);

                return Task.FromResult(new Result<EventLog>(eventLog));
            }
        }

        public Task<Result<Void>> TruncateEvents(EntityId eid, IncrementId since, CancellationToken ct) {
            lock (_eventLogs) {
                if (!_eventLogs.TryGetValue(eid, out var entry))
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

                _eventLogs[eid] = newEntry;
                
                return Task.FromResult(new Result<Void>(Void.Value));
            }
        }
    }
}