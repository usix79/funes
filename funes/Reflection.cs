using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    
    public enum ReflectionStatus { Opinion = -1, Fallacy = 0, Truth = 1, Lost = 404 }
    
    public readonly struct ReflectionId : IEquatable<ReflectionId>, IComparable<ReflectionId>, IComparable {
        public int CompareTo(object? obj) {
            if (ReferenceEquals(null, obj)) return 1;
            return obj is ReflectionId other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(ReflectionId)}");
        }
        
        public string Id { get; }

        public static readonly ReflectionId Singularity = new ("");
        public static readonly ReflectionId None = new ("");
        private static readonly DateTimeOffset FryReawakening = 
            new (3000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public ReflectionId(string id) => Id = id;
        
        public static long MillisecondsBeforeFryReawakening(DateTimeOffset dt) => 
            Convert.ToInt64((FryReawakening - dt).TotalMilliseconds);

        private const int DigitsLength = 14;
        private const int TailLenght = 6;

        private static readonly ThreadLocal<Random> Rand = new (() => new Random(DateTime.Now.Millisecond));
        
        public static ReflectionId ComposeId(DateTimeOffset dt, Random? rand) {
            Debug.Assert(rand != null, nameof(rand) + " != null");

            var id = string.Create(DigitsLength + 1 + TailLenght, MillisecondsBeforeFryReawakening(dt), 
                (span, num) => {
                    for (var i = 0; i < DigitsLength; i++, num /= 10) {
                        span[DigitsLength - i - 1] = (char)('0' + num % 10);
                    }
                    span[DigitsLength] = '-';
                    for (var i = 0; i < TailLenght; i++) {
                        span[DigitsLength + i + 1] = (char) ('a' + rand.Next(25));
                    }
                });
            
            return new ReflectionId(id);
        }
        
        public static ReflectionId NewId() => ComposeId(DateTimeOffset.UtcNow, Rand.Value);
        public bool Equals(ReflectionId other) => Id == other.Id;
        public override bool Equals(object? obj) => obj is ReflectionId other && Equals(other);
        public override int GetHashCode() => Id.GetHashCode();
        public static bool operator ==(ReflectionId left, ReflectionId right) => left.Equals(right);
        public static bool operator !=(ReflectionId left, ReflectionId right) => !left.Equals(right);
        public int CompareTo(ReflectionId other) => string.Compare(Id, other.Id, StringComparison.Ordinal);
        public override string ToString() => $"RID:{Id}";
    }
    
    public record Reflection(
        ReflectionId Id, ReflectionId ParentId, ReflectionStatus Status,
        MemId Fact, MemKey[] Premises, MemId[] Conclusions,
        IReadOnlyDictionary<string, string> Details
        ) {
        
        public static readonly string Category = "funes/reflections";
        public static string DetailsStartTime = "StartTime";
        public static string DetailsReflectTime = "ReflectTime";
        public static string DetailsRepoTime = "RepoTime";
        public static string DetailsAttempt = "Attempt";
        // private const int DefaultTtl = 360; // 1 hour

        public static MemId CreateMemId(ReflectionId rid) => new MemId(Category, rid.Id);
        public static MemKey CreateMemKey(ReflectionId rid) => new MemKey(CreateMemId(rid), rid);
        
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

        public static async ValueTask<Result<Reflection>> Load(Mem.IRepository repo, ReflectionId rid) {
            var getResult = await repo.Get(CreateMemKey(rid), Decoder);
            return getResult.IsOk
                ? new Result<Reflection>((Reflection) getResult.Value.Value)
                : new Result<Reflection>(getResult.Error);
        }

        public struct Fallacy {
            public MemKey PremiseKey { get;}
            public ReflectionId ActualRid { get;}
            public Fallacy(MemKey premise, ReflectionId actualRid) => (PremiseKey, ActualRid) = (premise, actualRid);
        }
        
        public static async ValueTask<Result<Fallacy[]>> Check (Mem.IRepository repo, ReflectionId rid) {
            var loadResult = await Load(repo, rid);
            if (loadResult.IsError) return new Result<Fallacy[]>(loadResult.Error);

            var reflection = loadResult.Value;
            
            var historyTasks = reflection.Premises
                .Select(premiseKey => repo.GetHistory(premiseKey.Id, reflection.Id, 1).AsTask());
            
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
            public MemId MemId { get;}
            public ReflectionId[] Heads { get;}
            public Fork(MemId memId, ReflectionId[] heads) => (MemId, Heads) = (memId, heads);
        }

        public static ValueTask<Result<Fork[]>> FindForks(Mem.IRepository repo, ReflectionId since) {
            throw new NotImplementedException();
        }
    }
}