using System;
using System.ComponentModel;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.IO;
using StackExchange.Redis;

namespace Funes.Redis {
    public static class RedisHelpers {

        public class KeysHolder {
            public RedisKey[] Arr { get; }

            public KeysHolder(int size) {
                Arr = new RedisKey[size];
            }
        }

        public class ValuesHolder {
            public RedisValue[] Arr { get; }

            public ValuesHolder(int size) {
                Arr = new RedisValue[size];
            }
        }

        public readonly struct Script {
            public string Source { get; }
            public byte[] Digest { get; }

            public Script(string source) {
                Source = source;
                Digest = RedisHelpers.Digest(source);
            }
        }
        
        public static byte[] Digest(string script) {
            using var sha1 = SHA1.Create();
            return sha1.ComputeHash(Encoding.UTF8.GetBytes(script));
        }
    
        public static async Task<Result<RedisResult?>> Eval(ConnectionMultiplexer redis, ILogger logger,
            Script script, RedisKey[] keys, RedisValue[] values) {
            RedisResult? result = null;
            try {
                var db = redis.GetDatabase();
                result = await db.ScriptEvaluateAsync (script.Digest, keys, values);
                
            }
            catch (Exception x ) {
                if (x.Message.StartsWith("NOSCRIPT")) {
                    logger.LogInformation($"Got NOSCRIPT from redis server, reevaluating ```{script.Source}```");
                    try {
                        var db = redis.GetDatabase();
                        result = await db.ScriptEvaluateAsync(script.Source, keys, values);
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