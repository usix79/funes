using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using static Funes.Redis.RedisHelpers;

namespace Funes.Redis {
    public class RedisTransactionEngine : ITransactionEngine {
        private const string Prefix = "TRE:";
        private static readonly RedisKey PrefixKey = Prefix; 
        private readonly ILogger _logger;
        private readonly ConnectionMultiplexer _redis;
        private readonly string _commitScript;
        private readonly byte[] _commitScriptDigest;

        public RedisTransactionEngine(string connectionString, ILogger logger, int ttl = 3600) {
            _logger = logger;
            _redis = ConnectionMultiplexer.Connect(connectionString);
            _commitScript = ComposeCommitScript(ttl);
            _commitScriptDigest = Digest(_commitScript);
        }

        public async Task<Result<Void>> TryCommit(ArraySegment<EntityStampKey> premises,
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct) {

            if (conclusions.Count == 0) 
                return new Result<Void>(Void.Value);

            var keys = new RedisKey[premises.Count + conclusions.Count];
            var values = new RedisValue[premises.Count + 3];
            values[0] = premises.Count;
            values[1] = conclusions.Count;
            values[2] = incId.Id;

            var index = 0;
            foreach(var stampKey in premises) {
                values[3 + index] = stampKey.IncId.Id;
                keys[index] = PrefixKey.Append(stampKey.EntId.Id);
                index++;
            }
            foreach (var entId in conclusions) {
                keys[index++] = PrefixKey.Append(entId.Id);
            }

            var result = await Eval(_redis, _logger, _commitScriptDigest, _commitScript, keys, values);

            if (result.IsError) 
                return new Result<Void>(result.Error);

            if (result.Value!.IsNull) 
                return new Result<Void>(Void.Value);
            
            var conflictArr = (string[]) result.Value;
            EntityStampKey? conflictedStampKey = null;
            foreach (var stampKey in premises) {
                if (string.CompareOrdinal(stampKey.EntId.Id, 0,
                    conflictArr[0], Prefix.Length, stampKey.EntId.Id.Length) == 0) {
                    conflictedStampKey = stampKey;
                    break;
                }
            }
            if (!conflictedStampKey.HasValue) {
                return new Result<Void>(new Error.IoError(
                    $"Cannot find conflicted stamp for {conflictArr[0]}, {conflictArr[1]}"));
            }
            
            var conflict = new Error.CommitError.Conflict {
                EntId = conflictedStampKey.Value.EntId,
                PremiseIncId = conflictedStampKey.Value.IncId,
                ActualIncId = new IncrementId(conflictArr[1])
            };
            return new Result<Void>(new Error.CommitError(new[] {conflict}));
        }

        private static string ComposeCommitScript(int ttl) => @$"
local n1 = ARGV[1]
local n2 = ARGV[2]
local newIncId = ARGV[3]
for i=1,n1,1 do
    local actualIncId = redis.call('GET', KEYS[i])
    if (actualIncId) then 
        redis.call('EXPIRE', KEYS[i], {ttl})
        if (actualIncId ~= ARGV[3 + i]) then 
            return {{KEYS[i], actualIncId}} 
        end
    end    
end
for i=n1+1,n1+n2,1 do
    redis.call('SETEX', KEYS[i], {ttl}, newIncId)
end";
    }
}