using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes {
    public record Reflection(
        ReflectionId Id, ReflectionId ParentId, ReflectionStatus Status,
        EntityId Fact, EntityStampKey[] Premises, EntityId[] Conclusions,
        IReadOnlyDictionary<string, string> Details
        ) {
        
        public static readonly string Category = "funes/reflections";
        public static string DetailsStartTime = "StartTime";
        public static string DetailsReflectTime = "ReflectTime";
        public static string DetailsRepoTime = "RepoTime";
        public static string DetailsAttempt = "Attempt";
        // private const int DefaultTtl = 360; // 1 hour

        public static EntityId CreateMemId(ReflectionId rid) => new EntityId(Category, rid.Id);
        public static EntityStampKey CreateMemKey(ReflectionId rid) => new EntityStampKey(CreateMemId(rid), rid);
        
        // public interface ISourceOfTruth {
        //     ValueTask<Result<ReflectionId>> GetActualRid(MemId id);
        //     ValueTask<Result<ReflectionId>[]> GetActualRids(IEnumerable<MemId> ids);
        //     ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions);
        // }
        
        // public static async ValueTask<Result<Reflection>> Reflect (
        //     Mem.IRepository repo, Mem.ICache cache, ISourceOfTruth sot, Mem.IRepository.Encoder encoder,
        //     ReflectionId parentId,
        //     Mem fact,
        //     IEnumerable<MemKey> premises,
        //     IEnumerable<Mem> conclusions,
        //     IEnumerable<KeyValuePair<string,string>> details) {
        //
        //     var reflectTime = DateTime.UtcNow;
        //
        //     try {
        //         var rid = ReflectionId.NewId();
        //         var premisesArr = premises as MemKey[] ?? premises.ToArray();
        //         var conclusionsArr = conclusions.Select(mem => new MemStamp(mem, rid)).ToArray();
        //         
        //         var sotResult = await sot.TrySetConclusions(premisesArr, conclusionsArr.Select(x => x.Key));
        //         
        //         var status = sotResult.IsOk ? ReflectionStatus.Truth : ReflectionStatus.Fallacy;
        //
        //         var cacheError = Error.No; 
        //         if (sotResult.IsOk) {
        //             // TODO: avoid double serializing
        //             // serialize to ReadOnlyMemory or stream and pass to cache and repo
        //             var cacheResult = await cache.Put(conclusionsArr, DefaultTtl, encoder);
        //             if (cacheResult.IsError) {
        //                 status = ReflectionStatus.Lost;
        //                 cacheError = cacheResult.Error;
        //                 // TODO: rollback truth
        //                 // need return MemKey[] of the conclusions predecessors from  TrySetConclusions
        //             }
        //         }
        //
        //         var detailsDict = new Dictionary<string,string>(details);
        //         
        //         var factMem = new MemStamp(fact, rid);
        //         var reflection = new Reflection(rid, parentId, status, 
        //             fact.Id, premisesArr, conclusionsArr.Select(x => x.Key.Id).ToArray(), detailsDict);
        //         var reflectionMem = new MemStamp(new Mem(CreateMemId(rid), reflection), rid);
        //
        //         detailsDict[DetailsReflectTime] = reflectTime.ToFileTimeUtc().ToString();
        //         detailsDict[DetailsRepoTime] = DateTime.UtcNow.ToFileTimeUtc().ToString();
        //         
        //         var repoTasks =
        //             conclusionsArr
        //                 .Select(mem => repo.Put(mem, encoder).AsTask())
        //                 .Append(repo.Put(factMem, encoder).AsTask())
        //                 .Append(repo.Put(reflectionMem, Encoder).AsTask());
        //
        //         var repoResults = await Task.WhenAll(repoTasks);
        //
        //         var errors = repoResults.Where(x => x.IsError).Select(x => x.Error);
        //         if (cacheError != Error.No) errors = errors.Prepend(cacheError);
        //         if (sotResult.IsError) errors = errors.Prepend(sotResult.Error);
        //
        //         var errorsArray = errors.ToArray();
        //         return errorsArray.Length == 0
        //             ? new Result<Reflection>(reflection)
        //             : Result<Reflection>.ReflectionError(reflection, errorsArray);
        //     }
        //     catch (Exception e) {
        //         return Result<Reflection>.Exception(e);
        //     }
        // }

        public static async ValueTask<Result<string>> Encoder(Stream output, object content) {
            try {
                await JsonSerializer.SerializeAsync(output, content);
                return new Result<string>("json");
            }
            catch (Exception e) {
                return Result<string>.SerdeError(e.Message);
            }
        }

        public static async ValueTask<Result<object>> Decoder(Stream input, string encoding) {
            if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);
            try {
                var reflectionOrNull = await JsonSerializer.DeserializeAsync<Reflection>(input);
                return reflectionOrNull != null
                    ? new Result<object>(reflectionOrNull)
                    : Result<object>.SerdeError("null");
            }
            catch (Exception e) {
                return Result<object>.SerdeError(e.Message);
            }
        }

        public static async ValueTask<Result<Reflection>> Load(IRepository repo, ISerializer serializer, ReflectionId rid) {
            var getResult = await repo.Get(CreateMemKey(rid), serializer);
            return getResult.IsOk
                ? new Result<Reflection>((Reflection) getResult.Value.Value)
                : new Result<Reflection>(getResult.Error);
        }

        public struct Fallacy {
            public EntityStampKey PremiseKey { get;}
            public ReflectionId ActualRid { get;}
            public Fallacy(EntityStampKey premise, ReflectionId actualRid) => (PremiseKey, ActualRid) = (premise, actualRid);
        }
        
        public static async ValueTask<Result<Fallacy[]>> Check (IRepository repo, ISerializer serializer, ReflectionId rid) {
            var loadResult = await Load(repo, serializer, rid);
            if (loadResult.IsError) return new Result<Fallacy[]>(loadResult.Error);

            var reflection = loadResult.Value;
            
            var historyTasks = reflection.Premises
                .Select(premiseKey => repo.GetHistory(premiseKey.Eid, reflection.Id, 1).AsTask());
            
            var historyItems = await Task.WhenAll(historyTasks);

            var errors = historyItems.Where(x => x.IsError).Select(x => x.Error).ToArray();
            if (errors.Length > 0) return Result<Fallacy[]>.AggregateError(errors);
            
            var fallacies =
                reflection.Premises
                    .Zip(historyItems, (premiseKey, historyResult) => (premiseKey, historyResult.Value.FirstOrDefault()))
                    .Where(x => x.Item1.Rid.CompareTo(x.Item2) > 0)
                    .Select(x => new Fallacy(x.Item1, x.Item2));
            
            return new (fallacies.ToArray());
        }

        public struct Fork {
            public EntityId EntityId { get;}
            public ReflectionId[] Heads { get;}
            public Fork(EntityId entityId, ReflectionId[] heads) => (EntityId, Heads) = (entityId, heads);
        }

        public static ValueTask<Result<Fork[]>> FindForks(IRepository repo, ReflectionId since) {
            throw new NotImplementedException();
        }
    }
}