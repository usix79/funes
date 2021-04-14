using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<Void>> Upload(EntityStamp stamp, ISerializer ser, CancellationToken ct, bool skipCache = false);

        ValueTask<Result<int>> AppendEvent(EntityId eid, Event evt, EntityId offsetEntId,
            CancellationToken ct, bool skipCache = false);

        ValueTask<Result<Void>> TryCommit(ArraySegment<EntityStampKey> premises, 
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct);
        
        Task Flush();
    }
}