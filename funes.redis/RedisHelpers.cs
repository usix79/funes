using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Funes.Redis {
    public static class RedisHelpers {
        public static byte[] Digest(string script) {
            using var sha1 = SHA1.Create();
            return sha1.ComputeHash(Encoding.UTF8.GetBytes(script));
        }
    
        public static async Task<Result<RedisResult?>> Eval(ConnectionMultiplexer redis, ILogger logger,
            byte[] digest, string script, RedisKey[] keys, RedisValue[] values) {
            RedisResult? result = null;
            try {
                var db = redis.GetDatabase();
                result = await db.ScriptEvaluateAsync (digest, keys, values);
                
            }
            catch (Exception x ) {
                if (x.Message.StartsWith("NOSCRIPT")) {
                    logger.LogInformation($"Got NOSCRIPT from redis server, reevaluating ```{script}```");
                    try {
                        var db = redis.GetDatabase();
                        result = await db.ScriptEvaluateAsync(script, keys, values);
                    }
                    catch(Exception xx) {
                        return new Result<RedisResult?>(new Error.ExceptionError(xx));
                    }
                }
                else {
                    return new Result<RedisResult?>(new Error.ExceptionError(x));
                }
            }
            return new Result<RedisResult?>(result);
        }
        
    }
    
}