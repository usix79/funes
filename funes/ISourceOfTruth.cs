using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ISourceOfTruth {
        public readonly struct CommitDetail {
            public EntityId Eid { get; }
            public CognitionId NewCid { get; } 
            public CognitionId OldCid { get; }            
        }
 
        Task<Result<CommitDetail[]>> Commit(IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct);
    }
}