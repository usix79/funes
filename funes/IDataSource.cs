using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataSource : IDataRetriever {
        public readonly struct CommitResult {
            public EntityId Eid { get; }
            public ReflectionId NewRid { get; } 
            public ReflectionId OldRid { get; }            
        }
        
        Task<Result<bool>> Upload(IEnumerable<EntityStamp> mems, ISerializer serializer, CancellationToken ct, bool skipCache = false);
        Task<Result<CommitResult[]>> Commit(IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct);
        Task<Result<bool>> Rollback(IEnumerable<CommitResult> commitResults, CancellationToken ct);
    }
}