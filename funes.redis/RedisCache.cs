using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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

        private readonly ILogger _logger;
        private readonly ConnectionMultiplexer _redis;
        private readonly TimeSpan _ttlSpan;
        private readonly string _updateScript;
        private readonly byte[] _updateScriptDigest;
        private readonly string _emptyScript;
        private readonly byte[] _emptyScriptDigest;

        public RedisCache(string connectionString, ILogger logger, int ttl = 3600) {
            _logger = logger;
            _ttlSpan = TimeSpan.FromSeconds(ttl);
            _redis = ConnectionMultiplexer.Connect(connectionString);
            _updateScript = ComposeUpdateScript(ttl);
            _updateScriptDigest = Digest(_updateScript);
            _emptyScript = ComposeUploadEmptyScript(ttl);
            _emptyScriptDigest = Digest(_emptyScript);
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

                    await using (var stream = new MemoryStream((byte[]) values[3])) {
                        var decodingResult = await ser.Decode(stream, eid, values[2]);
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
                var encodeResult = await ser.Encode(stream, entry.EntId, entry.Value);
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
                var encodeResult = await ser.Encode(stream, entry.EntId, entry.Value);
                if (encodeResult.IsError) {
                    return new Result<Void>(encodeResult.Error);
                }

                var result1 = await Eval(_redis, _logger, _updateScriptDigest, _updateScript, 
                    new[] {new RedisKey(entry.EntId.Id)}, new RedisValue[]
                        {(int) entry.Status, entry.IncId.Id, encodeResult.Value, RedisValue.CreateFrom(stream)});

                return result1.IsOk
                    ? new Result<Void>(Void.Value)
                    : new Result<Void>(result1.Error);
            }

            var result2 = await Eval(_redis, _logger, _emptyScriptDigest, _emptyScript, 
                new[] {new RedisKey(entry.EntId.Id)}, new RedisValue[] {(int) entry.Status});

            return result2.IsOk
                ? new Result<Void>(Void.Value)
                : new Result<Void>(result2.Error);
        }
    }
}