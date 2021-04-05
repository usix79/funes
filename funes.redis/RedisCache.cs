using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Funes.Redis {
    
    public class RedisCache : ICache {
        private const string PropIncId = "IncId";
        private const string PropStatus = "Status";
        private const string PropEncoding = "Enc";
        private const string PropData = "Data";
        private readonly RedisValue[] _hashFields = {new (PropStatus), new (PropIncId), new (PropEncoding), new (PropData)};

        private readonly ILogger _logger;
        private readonly ConnectionMultiplexer _redis;
        private readonly int _ttl;

        public RedisCache(string connectionString, ILogger logger, int ttl = 3600) {
            _logger = logger;
            _ttl = ttl;
            _redis = ConnectionMultiplexer.Connect(connectionString);
        }

        public async Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct) {
            var db = _redis.GetDatabase();

            RedisValue[]? values;
            try {
                values = await db!.HashGetAsync(eid.Id, _hashFields);
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
            
            var (encodingValue, dataValue, incIdValue) = (RedisValue.EmptyString, RedisValue.EmptyString, RedisValue.EmptyString);

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
                new (PropStatus, (int)entry.Status),
                new (PropIncId, incIdValue),
                new (PropEncoding, encodingValue),
                new (PropData, dataValue)
            };

            try {
                await db!.HashSetAsync(entry.EntId.Id, hashEntries);
                await db!.KeyExpireAsync(entry.EntId.Id, TimeSpan.FromSeconds(_ttl));
                return new Result<Void>(Void.Value);
            }
            catch (Exception x) {
                return new Result<Void>(new Error.ExceptionError(x));
            }
        }

        public Task<Result<bool>> UpdateIfNewer(IEnumerable<EntityEntry> entries, ISerializer ser, CancellationToken ct) => throw new System.NotImplementedException();
    }
}