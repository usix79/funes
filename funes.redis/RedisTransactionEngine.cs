using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using static Funes.Redis.RedisHelpers;

namespace Funes.Redis {
    public class RedisTransactionEngine : ITransactionEngine {
        private static string _prefix = "TRE:"; 
        private static RedisKey _prefixKey = _prefix; 
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

        public async Task<Result<Void>> TryCommit(IEnumerable<EntityStampKey> inputs, 
            IEnumerable<EntityId> outputs, IncrementId incId, CancellationToken ct) {
            var inputsArr = inputs as EntityStampKey[] ?? inputs.ToArray();
            var inputsCount = inputsArr.Length;
            var outputsCount = outputs.Count();
            if (outputsCount == 0) return new Result<Void>(Void.Value);
            
            var keys = new RedisKey[inputsCount + outputsCount];
            var values = new RedisValue[inputsCount + 3];
            values[0] = inputsCount;
            values[1] = outputsCount;
            values[2] = incId.Id;
            for(var i = 0; i < inputsCount; i++) {
                keys[i] = _prefixKey.Append(inputsArr[i].EntId.Id);
                values[3 + i] = inputsArr[i].IncId.Id;
            }

            var index = inputsCount;
            foreach (var entId in outputs) {
                keys[index++] = _prefixKey.Append(entId.Id);
            }

            var result = await Eval(_redis, _logger, _commitScriptDigest, _commitScript, keys, values);

            if (result.IsOk) {
                if (result.Value!.IsNull) return new Result<Void>(Void.Value);
                var conflictArr = (string[]) result.Value;
                var conflictedStamp = inputsArr.FirstOrDefault(stamp => 
                    string.CompareOrdinal(stamp.EntId.Id, 0, 
                        conflictArr[0], _prefix.Length, stamp.EntId.Id.Length) == 0);
                
                if (conflictedStamp.IsNull) {
                    return new Result<Void>(new Error.IoError(
                        $"Cannot find conflicted stamp for {conflictArr[0]}, {conflictArr[1]}"));
                }
                var conflict = new Error.CommitError.Conflict {
                    EntId = conflictedStamp.EntId,
                    PremiseIncId = conflictedStamp.IncId,
                    ActualIncId = new IncrementId(conflictArr[1])
                };
                return new Result<Void>(new Error.CommitError(new[] {conflict}));
            }
            
            return new Result<Void>(result.Error);
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