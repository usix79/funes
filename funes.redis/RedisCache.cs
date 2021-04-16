using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using static Funes.Redis.RedisHelpers;

namespace Funes.Redis {
    
    public class RedisCache : ICache {
        private const string PropIncId = "IncId";
        private const string PropStatus = "Status";
        private const string PropEncoding = "Enc";
        private const string PropData = "Data";
        private readonly RedisValue[] _hashFields 
            = {new (PropStatus), new (PropIncId), new (PropEncoding), new (PropData)};
        private static RedisKey EventsLockPrefix = "EVTL:" ;
        private static RedisKey EventsDataPrefix = "EVTD:" ;
        private static RedisKey EventsIncPrefix = "EVTI:";

        private static readonly ObjectPool<KeysHolder> Keys1Holders = new (() => new KeysHolder(1), 7);
        private static readonly ObjectPool<KeysHolder> Keys3Holders = new (() => new KeysHolder(3), 7);
        private static readonly ObjectPool<ValuesHolder> Values1Holders = new (() => new ValuesHolder(1), 7);
        private static readonly ObjectPool<ValuesHolder> Values2Holders = new (() => new ValuesHolder(2), 7);
        private static readonly ObjectPool<ValuesHolder> Values4Holders = new (() => new ValuesHolder(4), 7);

        private readonly ILogger _logger;
        private readonly ConnectionMultiplexer _redis;
        private readonly TimeSpan _ttlSpan;
        private readonly Script _updateScript;
        private readonly Script _emptyScript;
        private readonly Script _updateEventsScript;
        private readonly Script _getEventLogScript;
        private readonly Script _appendEventScript;
        private readonly Script _truncateEventScript;

        public RedisCache(string connectionString, ILogger logger, int ttl = 3600) {
            _logger = logger;
            _ttlSpan = TimeSpan.FromSeconds(ttl);
            _redis = ConnectionMultiplexer.Connect(connectionString);
            _updateScript = new Script(ComposeUpdateScript(ttl));
            _emptyScript = new Script(ComposeUploadEmptyScript(ttl));
            _updateEventsScript = new Script(ComposeUpdateEventsScript(ttl));
            _getEventLogScript = new Script(ComposeGetEventsLogScript(ttl));
            _appendEventScript = new Script(ComposeAppendEventScript(ttl));
            _truncateEventScript = new Script(ComposeTruncateEventsScript(ttl));
        }

        public async Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct) {
            var db = _redis.GetDatabase();

            RedisValue[]? values;
            try {
                var tran = db!.CreateTransaction();
                var t1 = tran!.HashGetAsync(eid.Id, _hashFields);
                var t2 = tran!.KeyExpireAsync(eid.Id, _ttlSpan);
                var committed = await tran.ExecuteAsync();
                if (!committed) {
                    return new Result<EntityEntry>(new Error.IoError("redis hash get failed"));
                }
                values = await t1;
            }
            catch (Exception x) {
                return new Result<EntityEntry>(new Error.ExceptionError(x));
            }

            if (values[0].IsNull) {
                return new Result<EntityEntry>(Error.NotFound);
            }

            var status = (EntityEntry.EntryStatus?) (int?) values[0];
            switch (status) {
                case EntityEntry.EntryStatus.IsOk:
                    if (values[2].IsNull) {
                        _logger.LogError($"Cache entry with absent encoding for {eid}");
                        return new Result<EntityEntry>(EntityEntry.NotAvailable(eid));
                    }

                    if (values[3].IsNull) {
                        _logger.LogError($"Cache entry with absent data field for {eid}");
                        return new Result<EntityEntry>(EntityEntry.NotAvailable(eid));
                    }

                    var buffer = (byte[]) values[3];
                    await using (var stream = new MemoryStream(buffer, 0, buffer.Length, false, true)) {
                        var decodingResult = await ser.Decode(stream, eid, values[2], ct);
                        if (decodingResult.IsError) {
                            return new Result<EntityEntry>(decodingResult.Error);
                        }

                        return new Result<EntityEntry>(EntityEntry.Ok(new Entity(eid, decodingResult.Value),
                            new IncrementId(values[1])));
                    }
                case EntityEntry.EntryStatus.IsNotExist:
                    return new Result<EntityEntry>(EntityEntry.NotExist(eid));
                case EntityEntry.EntryStatus.IsNotAvailable:
                    return new Result<EntityEntry>(EntityEntry.NotAvailable(eid));
                default:
                    _logger.LogError($"Cache entry with unknown status={status} for {eid}");
                    return new Result<EntityEntry>(EntityEntry.NotAvailable(eid));
            }
        } 

        public async Task<Result<Void>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct) {
            var db = _redis.GetDatabase();
            
            var (encodingValue, dataValue, incIdValue) = 
                (RedisValue.EmptyString, RedisValue.EmptyString, RedisValue.EmptyString);

            if (entry.Status == EntityEntry.EntryStatus.IsOk) {
                await using var stream = new MemoryStream();
                var encodeResult = await ser.Encode(stream, entry.EntId, entry.Value, ct);
                if (encodeResult.IsError) {
                    return new Result<Void>(encodeResult.Error);
                }

                incIdValue = new RedisValue(entry.IncId.Id);
                encodingValue = new RedisValue(encodeResult.Value);
                dataValue = RedisValue.CreateFrom(stream);
            }

            var hashEntries = new HashEntry[] {
                new(PropStatus, (int) entry.Status),
                new(PropIncId, incIdValue),
                new(PropEncoding, encodingValue),
                new(PropData, dataValue)
            };

            try {
                var tran = db!.CreateTransaction();
                var t1 = tran!.HashSetAsync(entry.EntId.Id, hashEntries);
                var t2 = tran!.KeyExpireAsync(entry.EntId.Id, _ttlSpan);
                var committed = await tran.ExecuteAsync();

                return committed
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(new Error.IoError("redis hash set failed"));
            }
            catch (Exception x) {
                return new Result<Void>(new Error.ExceptionError(x));
            }
        }
        
        private static string ComposeUpdateScript(int ttl) =>
            @$"local incId = redis.call('HGET', KEYS[1], '{PropIncId}')
if (incId == false or incId == '' or ARGV[2] < incId) then
    redis.call('HMSET', KEYS[1], '{PropStatus}', ARGV[1], '{PropIncId}', ARGV[2], '{PropEncoding}', ARGV[3], '{PropData}', ARGV[4])
    redis.call('EXPIRE', KEYS[1], {ttl})
    return 1
else
    return 0
end";
        private static string ComposeUploadEmptyScript(int ttl) =>
            @$"local incId = redis.call('HGET', KEYS[1], '{PropIncId}')
if (incId == false) then
    redis.call('HMSET', KEYS[1], '{PropStatus}', ARGV[1], '{PropIncId}', '', '{PropEncoding}', '', '{PropData}', '')
    redis.call('EXPIRE', KEYS[1], {ttl})
    return 1
else
    return 0
end";

        public async Task<Result<Void>> UpdateIfNewer(EntityEntry entry, ISerializer ser, CancellationToken ct) {
            if (entry.IsOk) {
                await using var stream = new MemoryStream();
                var encodeResult = await ser.Encode(stream, entry.EntId, entry.Value, ct);
                if (encodeResult.IsError) {
                    return new Result<Void>(encodeResult.Error);
                }

                var keys = Keys1Holders.Rent();
                var values = Values4Holders.Rent();
                try {
                    keys.Arr[0] = entry.EntId.Id;
                    values.Arr[0] = (int) entry.Status;
                    values.Arr[1] = entry.IncId.Id;
                    values.Arr[2] = encodeResult.Value;
                    values.Arr[3] = RedisValue.CreateFrom(stream);
                    
                    var result1 = await Eval(_redis, _logger, _updateScript, keys.Arr, values.Arr);

                    return result1.IsOk
                        ? new Result<Void>(Void.Value)
                        : new Result<Void>(result1.Error);

                }
                finally {
                    Keys1Holders.Return(keys);
                    Values4Holders.Return(values);
                }
            }

            var keysForEmpty = Keys1Holders.Rent();
            var valuesForEmpty = Values1Holders.Rent();
            try {
                keysForEmpty.Arr[0] = entry.EntId.Id;
                valuesForEmpty.Arr[0] = (int) entry.Status;
                var result2 = await Eval(_redis, _logger, _emptyScript, keysForEmpty.Arr, valuesForEmpty.Arr);

                return result2.IsOk
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(result2.Error);
            }
            finally {
                Keys1Holders.Return(keysForEmpty);
                Values1Holders.Return(valuesForEmpty);
            }
        }

        public async Task<Result<EventLog>> GetEventLog(EntityId eid, CancellationToken ct) {

            var keys = Keys3Holders.Rent();
            try {
                SetEventKeys(keys, eid);
                var valuesArr = Array.Empty<RedisValue>();

                var result = await Eval(_redis, _logger, _getEventLogScript, keys.Arr, valuesArr);

                if (result.IsError) return new Result<EventLog>(result.Error);

                if (result.Value!.Type == ResultType.Integer) {
                    return ((int) result.Value) switch {
                        0 => new Result<EventLog>(Error.NotFound),
                        _ => new Result<EventLog>(EventLog.Empty)
                    };
                }

                var results = (RedisResult[]) result.Value;
                if (results.Length != 3) return Result<EventLog>.IoError("GetEventLog RedisResult should contain 3 elements");

                var first = new IncrementId((string) results[0]);
                var last = new IncrementId((string) results[1]);
                var data = (byte[]) results[2];
                return new Result<EventLog>(new EventLog(first, last, data));
            }
            finally {
                Keys3Holders.Return(keys);
            }
        }

        private static string ComposeGetEventsLogScript(int ttl) =>
            @$"if (redis.call('EXISTS', KEYS[1]) == 0) then return 0 end
if (redis.call('EXISTS', KEYS[2], KEYS[3]) < 2) then return 1 end

local first = redis.call('LINDEX', KEYS[2], 0)
local last = redis.call('LINDEX', KEYS[2], -1)
local dataArr = redis.call('LRANGE', KEYS[3], 0, -1)
local data = table.concat(dataArr)
redis.call('EXPIRE', KEYS[1], {ttl})
redis.call('EXPIRE', KEYS[2], {ttl+60})
redis.call('EXPIRE', KEYS[3], {ttl+60})

return {{first, last, data}}";


        public async Task<Result<Void>> UpdateEventsIfNotExists(EntityId eid, Event[] events, CancellationToken ct) {
            var keys = Keys3Holders.Rent();
            try {
                SetEventKeys(keys, eid);
                var valuesArr = new RedisValue[events.Length * 2];
                for (var i = 0; i < events.Length; i++) {
                    valuesArr[i] = events[i].IncId.Id;
                    valuesArr[events.Length + i] = events[i].Data;
                }
                
                var result = await Eval(_redis, _logger, _updateEventsScript, keys.Arr, valuesArr);

                return result.IsOk
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(result.Error);
            }
            finally {
                Keys3Holders.Return(keys);
            }
        }

        private static string ComposeUpdateEventsScript(int ttl) =>
            @$"if (redis.call('EXISTS', KEYS[1]) > 0) then return 0 end

redis.call('SETEX', KEYS[1], {ttl}, 1)
redis.call('DEL', KEYS[2], KEYS[3])
local eventsCount = #ARGV / 2 
if (eventsCount > 0) then
    redis.call('RPUSH', KEYS[2], unpack(ARGV, 1, eventsCount))
    redis.call('RPUSH', KEYS[3], unpack(ARGV, eventsCount + 1, #ARGV))
    redis.call('EXPIRE', KEYS[2], {ttl+60})
    redis.call('EXPIRE', KEYS[3], {ttl+60})
end
return 1";
        
        public async Task<Result<int>> AppendEvent(EntityId eid, Event evt, CancellationToken ct) {
            var keys = Keys3Holders.Rent();
            var values = Values2Holders.Rent();
            try {
                SetEventKeys(keys, eid);
                values.Arr[0] = evt.IncId.Id;
                values.Arr[1] = evt.Data;

                var result = await Eval(_redis, _logger, _appendEventScript, keys.Arr, values.Arr);

                if (result.IsError) return new Result<int>(result.Error);
            
                if (result.Value!.Type != ResultType.Integer) 
                    return Result<int>.IoError("AppendEvent RedisResult should be integer");
            
                var count = (int) result.Value;
            
                return count > 0 ? new Result<int>(count) : Result<int>.NotFound;
            }
            finally {
                Keys3Holders.Return(keys);
                Values2Holders.Return(values);
            }
        }

        private static string ComposeAppendEventScript(int ttl) =>
            @$"if (redis.call('EXISTS', KEYS[1]) == 0) then return 0 end
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('RPUSH', KEYS[3], ARGV[2])
redis.call('EXPIRE', KEYS[1], {ttl})
redis.call('EXPIRE', KEYS[2], {ttl+60})
redis.call('EXPIRE', KEYS[3], {ttl+60})
local count = redis.call('LLEN', KEYS[2])
return count";
        
        public async Task<Result<Void>> TruncateEvents(EntityId eid, IncrementId since, CancellationToken ct) {
            
            var keys = Keys3Holders.Rent();
            var values = Values1Holders.Rent();

            try {
                SetEventKeys(keys, eid);
                values.Arr[0] = since.Id;
            
                var result = await Eval(_redis, _logger, _truncateEventScript, keys.Arr, values.Arr);

                return result.IsOk
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(result.Error);
            }
            finally {
                Keys3Holders.Return(keys);
                Values1Holders.Return(values);
            }
        }
        
        private static string ComposeTruncateEventsScript(int ttl) =>
            @$"if (redis.call('EXISTS', KEYS[1]) == 0) then return 0 end
local pos = redis.call('LPOS', KEYS[2], ARGV[1])
if (pos == false) then return -1 end
redis.call('LTRIM', KEYS[2], pos + 1, -1)
redis.call('LTRIM', KEYS[3], pos + 1, -1)
redis.call('EXPIRE', KEYS[1], {ttl})
redis.call('EXPIRE', KEYS[2], {ttl+60})
redis.call('EXPIRE', KEYS[3], {ttl+60})
return pos";

        private static void SetEventKeys(KeysHolder keys, EntityId eid) {
            keys.Arr[0] = EventsLockPrefix.Append(eid.Id);
            keys.Arr[1] = EventsIncPrefix.Append(eid.Id);
            keys.Arr[2] = EventsDataPrefix.Append(eid.Id);
        }
    }
    
}