using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funes {
    public record Increment(
        IncrementId Id, 
        IncrementId ParentId, 
        IncrementStatus Status,
        EntityId Fact, 
        List<KeyValuePair<EntityStampKey, bool>> Inputs, 
        EntityId[] Outputs,
        List<KeyValuePair<string,string>> Constants,
        List<string> SideEffects,
        Dictionary<string, string> Details
        ) {
        
        public const string Category = "funes/increments";
        public const string ChildrenCategory = "funes/children";
        public const string DetailsIncrementTime = "IncrementTime";
        public const string DetailsAttempt = "Attempt";
        public const string DetailsLogicDuration = "LogicDuration";
        public const string DetailsCommitDuration = "CommitDuration";
        public const string DetailsUploadDuration = "UploadDuration";
        public const string DetailsCommitErrors = "CommitErrors";

        public static EntityId CreateEntId(IncrementId incId) => new (Category, incId.Id);
        public static EntityStamp CreateEntStamp(Increment inc) =>
            new (new Entity(Increment.CreateEntId(inc.Id), inc), inc.Id);
        public static EntityId CreateChildEntId(IncrementId parentId) => new (ChildrenCategory, parentId.Id);

        public static EntityStamp CreateChildEntStamp(IncrementId incId, IncrementId parentId) =>
            new(new Entity(Increment.CreateChildEntId(parentId), null!), incId);        
        public static EntityStampKey CreateStampKey(IncrementId incId) => new (CreateEntId(incId), incId);

        private static async ValueTask<Result<Increment>> Load(IRepository repo, ISerializer serializer, IncrementId incId) {
            var getResult = await repo.Load(CreateStampKey(incId), serializer, default);
            return getResult.IsOk
                ? new Result<Increment>((Increment) getResult.Value.Value)
                : new Result<Increment>(getResult.Error);
        }

        public struct Conflict {
            public EntityStampKey PremiseKey { get;}
            public IncrementId ActualIncId { get;}
            public Conflict(EntityStampKey premise, IncrementId actualIncId) => 
                (PremiseKey, ActualIncId) = (premise, actualIncId);
        }
        
        public static async ValueTask<Result<Conflict[]>> Check (IRepository repo, ISerializer serializer, IncrementId incId) {
            var loadResult = await Load(repo, serializer, incId);
            if (loadResult.IsError) return new Result<Conflict[]>(loadResult.Error);

            var increment = loadResult.Value;
            
            var historyTasks = increment.Inputs.Select(pair => pair.Key)
                .Select(premiseKey => repo.History(premiseKey.EntId, increment.Id, 1).AsTask());
            
            var historyItems = await Task.WhenAll(historyTasks);

            var errors = historyItems.Where(x => x.IsError).Select(x => x.Error).ToArray();
            if (errors.Length > 0) return Result<Conflict[]>.AggregateError(errors);
            
            var conflicts =
                increment.Inputs.Select(pair => pair.Key)
                    .Zip(historyItems, (premiseKey, historyResult) => (premiseKey, historyResult.Value.FirstOrDefault()))
                    .Where(x => x.Item1.IncId.CompareTo(x.Item2) > 0)
                    .Select(x => new Conflict(x.Item1, x.Item2));
            
            return new (conflicts.ToArray());
        }

        public readonly struct Collision {
            public EntityId EntId { get;}
            public IncrementId[] Heads { get;}
            public Collision(EntityId entityId, IncrementId[] heads) => (EntId, Heads) = (entityId, heads);
        }

        public static ValueTask<Result<Collision[]>> FindForks(IRepository repo, IncrementId since) {
            throw new NotImplementedException();
        }
    }
}