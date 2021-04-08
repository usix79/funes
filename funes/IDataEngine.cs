using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<Void>> Upload(EntityStamp stamp, ISerializer ser, CancellationToken ct, bool skipCache = false);

        ValueTask<Result<int>> UploadEvents(IEnumerable<(EntityStamp, EntityId)> events, 
            ISerializer ser, CancellationToken ct, bool skipCache = false);

        ValueTask<Result<Void>> TryCommit(ArraySegment<EntityStampKey> premises, 
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct);
        Task Flush();
    }
}