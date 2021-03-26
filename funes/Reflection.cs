using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Funes {
    public record Reflection(
        ReflectionId Id, 
        ReflectionId ParentId, 
        ReflectionStatus Status,
        EntityId Fact, 
        EntityStampKey[] Premises, 
        EntityId[] Conclusions,
        NameValueCollection Constants,
        ReadOnlyCollection<string?> SideEffects,
        IReadOnlyDictionary<string, string> Details
        ) {
        
        public const string Category = "funes/reflections";
        public const string ChildrenCategory = "funes/children";
        public static string DetailsStartTime = "StartTime";
        public static string DetailsReflectTime = "ReflectTime";
        public static string DetailsAttempt = "Attempt";
        public static string DetailsLogicDuration = "LogicDuration";
        public static string DetailsCommitDuration = "CommitDuration";
        public static string DetailsUploadDuration = "UploadDuration";

        public static EntityId CreateEntityId(ReflectionId rid) => new (Category, rid.Id);
        public static EntityId CreateChildrenEntityId(ReflectionId parentId) => new (Category, parentId.Id);
        public static EntityStampKey CreateStampKey(ReflectionId rid) => new (CreateEntityId(rid), rid);
        
        // public interface ISourceOfTruth {
        //     ValueTask<Result<ReflectionId>> GetActualRid(MemId id);
        //     ValueTask<Result<ReflectionId>[]> GetActualRids(IEnumerable<MemId> ids);
        //     ValueTask<Result<bool>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions);
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
            var getResult = await repo.Get(CreateStampKey(rid), serializer);
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