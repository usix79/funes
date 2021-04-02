using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {
    public record Cognition(
        CognitionId Id, 
        CognitionId ParentId, 
        CognitionStatus Status,
        EntityId Fact, 
        List<KeyValuePair<EntityStampKey, bool>> Inputs, 
        EntityId[] Outputs,
        List<KeyValuePair<string,string>> Constants,
        List<string> SideEffects,
        Dictionary<string, string> Details
        ) {
        
        public const string Category = "funes/congnitions";
        public const string ChildrenCategory = "funes/children";
        public static string DetailsCognitionTime = "CognitionTime";
        public static string DetailsAttempt = "Attempt";
        public static string DetailsLogicDuration = "LogicDuration";
        public static string DetailsCommitDuration = "CommitDuration";
        public static string DetailsUploadDuration = "UploadDuration";
        public static string DetailsCommitErrors = "CommitErrors";

        public static EntityId CreateEntityId(CognitionId cid) => new (Category, cid.Id);
        public static EntityId CreateChildrenEntityId(CognitionId parentId) => new (Category, parentId.Id);
        public static EntityStampKey CreateStampKey(CognitionId cid) => new (CreateEntityId(cid), cid);
        
        public static async ValueTask<Result<Cognition>> Load(IRepository repo, ISerializer serializer, CognitionId cid) {
            var getResult = await repo.Load(CreateStampKey(cid), serializer, default);
            return getResult.IsOk
                ? new Result<Cognition>((Cognition) getResult.Value.Value)
                : new Result<Cognition>(getResult.Error);
        }

        public struct Fallacy {
            public EntityStampKey PremiseKey { get;}
            public CognitionId ActualCid { get;}
            public Fallacy(EntityStampKey premise, CognitionId actualCid) => (PremiseKey, ActualCid) = (premise, actualCid);
        }
        
        public static async ValueTask<Result<Fallacy[]>> Check (IRepository repo, ISerializer serializer, CognitionId cid) {
            var loadResult = await Load(repo, serializer, cid);
            if (loadResult.IsError) return new Result<Fallacy[]>(loadResult.Error);

            var reflection = loadResult.Value;
            
            var historyTasks = reflection.Inputs.Select(pair => pair.Key)
                .Select(premiseKey => repo.History(premiseKey.Eid, reflection.Id, 1).AsTask());
            
            var historyItems = await Task.WhenAll(historyTasks);

            var errors = historyItems.Where(x => x.IsError).Select(x => x.Error).ToArray();
            if (errors.Length > 0) return Result<Fallacy[]>.AggregateError(errors);
            
            var fallacies =
                reflection.Inputs.Select(pair => pair.Key)
                    .Zip(historyItems, (premiseKey, historyResult) => (premiseKey, historyResult.Value.FirstOrDefault()))
                    .Where(x => x.Item1.Cid.CompareTo(x.Item2) > 0)
                    .Select(x => new Fallacy(x.Item1, x.Item2));
            
            return new (fallacies.ToArray());
        }

        public struct Fork {
            public EntityId EntityId { get;}
            public CognitionId[] Heads { get;}
            public Fork(EntityId entityId, CognitionId[] heads) => (EntityId, Heads) = (entityId, heads);
        }

        public static ValueTask<Result<Fork[]>> FindForks(IRepository repo, CognitionId since) {
            throw new NotImplementedException();
        }
    }
}