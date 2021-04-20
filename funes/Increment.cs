using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public record Increment(
        IncrementId Id, 
        StampKey FactKey,
        IncrementArgs Args,
        List<EntityId> Outputs,
        List<KeyValuePair<string,string>> Constants,
        List<KeyValuePair<string,string>> Details
        ) {
        
        public const string Category = "funes/increments";
        public const string ChildrenCategory = "funes/children";
        public const string DetailsIncrementTime = "IncrementTime";
        public const string DetailsAttempt = "Attempt";
        public const string DetailsLogicDuration = "LogicDuration";
        public const string DetailsCommitDuration = "CommitDuration";
        public const string DetailsUploadDuration = "UploadDuration";
        public const string DetailsCommitErrors = "CommitErrors";
        public const string DetailsSideEffects = "SideEffects";
        public const string DetailsError = "Error";

        public static EntityId CreateEntId(IncrementId incId) => new (Category, incId.Id);
        public static bool IsIncrement(EntityId entId) => entId.Id.StartsWith(Category);
        public static EntityId CreateChildEntId(IncrementId parentId) => new (ChildrenCategory, parentId.Id);
        public static bool IsChild(EntityId entId) => entId.Id.StartsWith(ChildrenCategory);
        public static StampKey CreateStampKey(IncrementId incId) => new (CreateEntId(incId), incId);
        
        public string FindDetail(string key) {
            foreach(var pair in Details)
                if (pair.Key == key)
                    return pair.Value;

            return "";
        }
        
        public static Result<BinaryData> Encode(Increment increment) {
            using MemoryStream stream = new();
            using Utf8JsonWriter writer = new(stream);
                
            try {
                JsonSerializer.Serialize(writer, increment);
                if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                return new Result<BinaryData>(new BinaryData("json", buffer));
            }
            catch (Exception e) {
                return Result<BinaryData>.Exception(e);
            }
        }

        public static ValueTask<Result<Void>> Upload(IDataEngine de, Increment increment, CancellationToken ct) {
            var encodingResult = Encode(increment);
            if (encodingResult.IsError)
                return ValueTask.FromResult(new Result<Void>(encodingResult.Error));

            var stamp = new BinaryStamp(CreateStampKey(increment.Id), encodingResult.Value);
            return de.Upload(stamp, ct, true);
        }

        public static ValueTask<Result<Void>> UploadChild(IDataEngine de, IncrementId incId, IncrementId parentId,
            CancellationToken ct) {

            var stampKey = CreateChildEntId(parentId).CreateStampKey(incId);
            return de.Upload(new BinaryStamp(stampKey, BinaryData.Empty), ct, true);
        }
        
        public static Result<Increment> Decode(BinaryData data) {
            if ("json" != data.Encoding) 
                return Result<Increment>.NotSupportedEncoding(data.Encoding);
            
            try {
                var reflectionOrNull = JsonSerializer.Deserialize<Increment>(data.Memory.Span);
                return reflectionOrNull != null
                    ? new Result<Increment>(reflectionOrNull)
                    : Result<Increment>.SerdeError("null");
            }
            catch (Exception e) {
                return Result<Increment>.SerdeError(e.Message);
            }
        }
        
        // public struct Conflict {
        //     public StampKey PremiseKey { get;}
        //     public IncrementId ActualIncId { get;}
        //     public Conflict(StampKey premise, IncrementId actualIncId) => 
        //         (PremiseKey, ActualIncId) = (premise, actualIncId);
        // }
        
        // public static async ValueTask<Result<Conflict[]>> Check (IRepository repo, ISerializer serializer, IncrementId incId) {
        //     var loadResult = await Load(repo, serializer, incId);
        //     if (loadResult.IsError) return new Result<Conflict[]>(loadResult.Error);
        //
        //     var increment = loadResult.Value;
        //     
        //     var historyTasks = increment.Inputs.Select(pair => pair.Key)
        //         .Select(premiseKey => repo.HistoryBefore(premiseKey.EntId, increment.Id, 1).AsTask());
        //     
        //     var historyItems = await Task.WhenAll(historyTasks);
        //
        //     var errors = historyItems.Where(x => x.IsError).Select(x => x.Error).ToArray();
        //     if (errors.Length > 0) return Result<Conflict[]>.AggregateError(errors);
        //     
        //     var conflicts =
        //         increment.Inputs.Select(pair => pair.Key)
        //             .Zip(historyItems, (premiseKey, historyResult) => (premiseKey, historyResult.Value.FirstOrDefault()))
        //             .Where(x => x.Item1.IncId.CompareTo(x.Item2) > 0)
        //             .Select(x => new Conflict(x.Item1, x.Item2));
        //     
        //     return new (conflicts.ToArray());
        // }

        // public readonly struct Collision {
        //     public EntityId EntId { get;}
        //     public IncrementId[] Heads { get;}
        //     public Collision(EntityId entityId, IncrementId[] heads) => (EntId, Heads) = (entityId, heads);
        // }
        //
        // public static ValueTask<Result<Collision[]>> FindForks(IRepository repo, IncrementId since) {
        //     throw new NotImplementedException();
        // }
    }
}