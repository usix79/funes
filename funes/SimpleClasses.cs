using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes {
    
    public class SimpleSerializer<T> : ISerializer {
        public async ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content) {
            try {
                await JsonSerializer.SerializeAsync(output, content);
                return new Result<string>("json");
            }
            catch (Exception ex) {
                return Result<string>.Exception(ex);
            }
        }

        public async ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding) {
            try {
                if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);

                var content = await JsonSerializer.DeserializeAsync<T>(input);
                return content != null
                    ? new Result<object>(content)
                    : Result<object>.SerdeError($"Failed to deserialize instance of {typeof(T)}");
            }
            catch (Exception ex) {
                return Result<object>.Exception(ex);
            }
        }
    }

    public class SimpleRepository : IRepository {

        private readonly ConcurrentDictionary<EntityStampKey, EntityStamp> _memories = new();

        public ValueTask<Result<bool>> Put(EntityStamp entityStamp, ISerializer _) {

            _memories[entityStamp.Key] = entityStamp;

            return ValueTask.FromResult(new Result<bool>(true));
        }

        public ValueTask<Result<EntityStamp>> Get(EntityStampKey key, ISerializer _) {
            var result =
                _memories.TryGetValue(key, out var mem)
                    ? new Result<EntityStamp>(mem)
                    : Result<EntityStamp>.NotFound;

            return ValueTask.FromResult(result);
        }
        
        public ValueTask<Result<IEnumerable<CognitionId>>> GetHistory(EntityId id, CognitionId before, int maxCount = 1) {
            var result =
                _memories.Keys
                    .Where(key => key.Eid == id)
                    .OrderBy(key => key.Cid)
                    .SkipWhile(key => string.CompareOrdinal(key.Cid.Id, before.Id) <= 0)
                    .Take(maxCount)
                    .Select(key => key.Cid);

            return ValueTask.FromResult(new Result<IEnumerable<CognitionId>>(result));
        }
    }

    // public class SimpleCache : Mem.ICache {
    //     
    //     private readonly ConcurrentDictionary<MemId, MemStamp> _memories = new();
    //     
    //     public ValueTask<Result<bool>> Put(IEnumerable<MemStamp> mems, int ttl, Mem.IRepository.Encoder _) {
    //
    //         foreach (var mem in mems) {
    //             _memories[mem.Key.Id] = mem;
    //         }
    //         
    //         return ValueTask.FromResult(new Result<bool>(true));
    //     }
    //
    //     public ValueTask<Result<MemStamp>[]> Get(IEnumerable<(MemId, Mem.IRepository.Decoder)> ids) {
    //         var arr = ids as (MemId, Mem.IRepository.Decoder)[] ?? ids.ToArray();
    //         var results = new Result<MemStamp>[arr.Length];
    //         for (var i = 0; i < arr.Length; i++) {
    //             results[i] =
    //                 _memories.TryGetValue(arr[i].Item1, out var mem)
    //                     ? new Result<MemStamp>(mem)
    //                     : Result<MemStamp>.NotFound;
    //         }
    //         
    //         return ValueTask.FromResult(results);
    //     }
    // }
    
    // public class SimpleSourceOfTruth : Cognition.ITransactionEngine {
    //
    //     private readonly ConcurrentDictionary<MemId, CognitionId> _latest = new();
    //     private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1); 
    //     
    //     public ValueTask<Result<CognitionId>> GetActualCid(MemId id) {
    //         var result =
    //             _latest.TryGetValue(id, out var cid)
    //                 ? new Result<CognitionId>(cid)
    //                 : Result<CognitionId>.NotFound;
    //         return ValueTask.FromResult<Result<CognitionId>>(result);
    //     }
    //
    //     public ValueTask<Result<CognitionId>[]> GetActualCids(IEnumerable<MemId> ids) {
    //         var idsArray = ids as MemId[] ?? ids.ToArray();
    //         var results = new Result<CognitionId>[idsArray.Length];
    //         for (var i = 0; i < idsArray.Length; i++) {
    //             results[i] =
    //                 _latest.TryGetValue(idsArray[i], out var cid)
    //                     ? new Result<CognitionId>(cid)
    //                     : Result<CognitionId>.NotFound;
    //         }
    //         
    //         return ValueTask.FromResult(results);
    //     }
    //
    //     public async ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions) {
    //         await _lock.WaitAsync();
    //         try {
    //             var premisesKeys = premises as MemKey[] ?? premises.ToArray();
    //             var premisesIds = premisesKeys.Select(key => key.Id).ToArray();
    //             var actualConclusions = await GetActualCids(premisesIds);
    //
    //             for (var i = 0; i < premisesKeys.Length; i++){
    //                 var actual = actualConclusions[i];
    //                 if (actual.IsOk && actual.Value != premisesKeys[i].Cid 
    //                     || actual.IsError && actual.Error != Error.NotFound) {
    //                     return new Result<bool>(false);
    //                 }
    //             }
    //             
    //             foreach (var conclusion in conclusions) {
    //                 _latest[conclusion.Id] = conclusion.Cid;
    //             }
    //
    //             return new Result<bool>(true);
    //         }
    //         finally {
    //             _lock.Release();
    //         }
    //     }
    // }
}