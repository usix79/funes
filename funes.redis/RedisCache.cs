using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using static Funes.Redis.RedisHelpers;

namespace Funes.Redis {
    
    public class RedisCache : ICache {
        private const string PropIncId = "IncId";
        private const string PropEncoding = "Enc";
        private const string PropData = "Data";
        private readonly RedisValue[] _hashFields = { new (PropIncId), new (PropEncoding), new (PropData)};
        private static RedisKey EventsLockPrefix = "EVTL:" ;
        private static RedisKey EventsDataPrefix = "EVTD:" ;
        private static RedisKey EventsIncPrefix = "EVTI:";

        private static readonly Utils.ObjectPool<HashEntryHolder> HashEntry3Holders = new (() => new HashEntryHolder(3), 7);
        private static readonly Utils.ObjectPool<KeysHolder> Keys1Holders = new (() => new KeysHolder(1), 7);
        private static readonly Utils.ObjectPool<KeysHolder> Keys3Holders = new (() => new KeysHolder(3), 7);
        private static readonly Utils.ObjectPool<ValuesHolder> Values1Holders = new (() => new ValuesHolder(1), 7);
        private static readonly Utils.ObjectPool<ValuesHolder> Values2Holders = new (() => new ValuesHolder(2), 7);
        private static readonly Utils.ObjectPool<ValuesHolder> Values3Holders = new (() => new ValuesHolder(3), 7);

        private readonly ILogger _logger;
        private readonly ConnectionMultiplexer _redis;
        private readonly TimeSpan _ttlSpan;
        private readonly Script _updateScript;
        private readonly Script _updateEventsScript;
        private readonly Script _getEventLogScript;
        private readonly Script _appendEventScript;
        private readonly Script _truncateEventScript;

        public RedisCache(string connectionString, ILogger logger, int ttl = 3600) {
            _logger = logger;
            _ttlSpan = TimeSpan.FromSeconds(ttl);
            _redis = ConnectionMultiplexer.Connect(connectionString);
            _updateScript = new Script(ComposeUpdateScript(ttl));
            _updateEventsScript = new Script(ComposeUpdateEventsScript(ttl));
            _getEventLogScript = new Script(ComposeGetEventsLogScript(ttl));
            _appendEventScript = new Script(ComposeAppendEventScript(ttl));
            _truncateEventScript = new Script(ComposeTruncateEventsScript(ttl));
        }

        public async Task<Result<BinaryStamp>> Get(EntityId eid, CancellationToken ct) {
            var db = _redis.GetDatabase();

            RedisValue[]? values;
            try {
                var tran = db!.CreateTransaction();
                var t1 = tran!.HashGetAsync(eid.Id, _hashFields);
                var _ = tran!.KeyExpireAsync(eid.Id, _ttlSpan);
                var committed = await tran.ExecuteAsync();
                if (!committed) return Result<BinaryStamp>.IoError("redis hash get failed");
                values = await t1;
            }
            catch (Exception x) {
                return new Result<BinaryStamp>(new Error.ExceptionError(x));
            }

            if (values[0].IsNull)
                return new Result<BinaryStamp>(Error.NotFound);
            
            if (values[1].IsNull) {
                var msg = $"Cache entry with absent encoding for {eid}";
                _logger.LogError(msg);
                return Result<BinaryStamp>.IoError(msg);
            }
            
            if (values[2].IsNull) {
                var msg = $"Cache entry with absent data field for {eid}";
                _logger.LogError(msg);
                return Result<BinaryStamp>.IoError(msg);
            }
            
            var stampKey = new StampKey(eid, new IncrementId(values[0]));
            var stamp = new BinaryStamp(stampKey, new BinaryData(values[1], (byte[]) values[2]));
            return new Result<BinaryStamp>(stamp);
        } 

        public async Task<Result<Void>> Set(BinaryStamp stamp, CancellationToken ct) {
            var db = _redis.GetDatabase();

            var holder = HashEntry3Holders.Rent();
            try {
                holder.Arr[0] = new(PropIncId, stamp.IncId.Id);
                holder.Arr[1] = new(PropEncoding, stamp.Data.Encoding);
                holder.Arr[2] = new(PropData, stamp.Data.Memory);

                var tran = db!.CreateTransaction();
                var t1 = tran!.HashSetAsync(stamp.Eid.Id, holder.Arr);
                var t2 = tran!.KeyExpireAsync(stamp.Eid.Id, _ttlSpan);
                var committed = await tran.ExecuteAsync();

                return committed
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(new Error.IoError("redis hash set failed"));
            }
            catch (Exception x) {
                return new Result<Void>(new Error.ExceptionError(x));
            }
            finally {
                HashEntry3Holders.Return(holder);
            }
        }
        
        private static string ComposeUpdateScript(int ttl) =>
            @$"local incId = redis.call('HGET', KEYS[1], '{PropIncId}')
if (incId == false or incId == '' or ARGV[1] < incId) then
    redis.call('HMSET', KEYS[1], '{PropIncId}', ARGV[1], '{PropEncoding}', ARGV[2], '{PropData}', ARGV[3])
    redis.call('EXPIRE', KEYS[1], {ttl})
    return 1
else
    return 0
end";
        public async Task<Result<Void>> UpdateIfNewer(BinaryStamp stamp, CancellationToken ct) {
            var keys = Keys1Holders.Rent();
            var values = Values3Holders.Rent();
            try {
                keys.Arr[0] = stamp.Eid.Id;
                values.Arr[0] = stamp.IncId.Id;
                values.Arr[1] = stamp.Data.Encoding;
                values.Arr[2] = stamp.Data.Memory;
                
                var result1 = await Eval(_redis, _logger, _updateScript, keys.Arr, values.Arr);

                return result1.IsOk
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(result1.Error);

            }
            finally {
                Keys1Holders.Return(keys);
                Values3Holders.Return(values);
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