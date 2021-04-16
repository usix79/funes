using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<Void>> Upload(EntityStamp stamp, ISerializer ser, CancellationToken ct, bool skipCache = false);

        ValueTask<Result<int>> AppendEvent(EntityId recordId, Event evt, EntityId offsetId,
            CancellationToken ct, bool skipCache = false);
        
        ValueTask<Result<Void>> TruncateEvents(EntityId recordId, EntityStampKey offsetKey, 
            IncrementId lastToTruncate, CancellationToken ct);
        
        ValueTask<Result<Void>> TryCommit(ArraySegment<EntityStampKey> premises, 
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct);
        
        Task Flush();
    }
}