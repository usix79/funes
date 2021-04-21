using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<Void>> Upload(BinaryStamp stamp, CancellationToken ct, bool skipCache = false);

        ValueTask<Result<int>> AppendEvent(EntityId recordId, Event evt, EntityId offsetId,
            CancellationToken ct, bool skipCache = false);
        
        ValueTask<Result<Void>> TruncateEvents(EntityId recordId, IncrementId lastToTruncate, CancellationToken ct);
        
        ValueTask<Result<Void>> TryCommit(ArraySegment<StampKey> premises, 
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct);
        
        ValueTask Flush(CancellationToken ct = default);
    }
}