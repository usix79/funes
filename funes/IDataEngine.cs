using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<Void>> Upload(IEnumerable<EntityStamp> stamps, 
            ISerializer ser, CancellationToken ct, bool skipCache = false);
        ValueTask<Result<Void>> TryCommit(IEnumerable<EntityStampKey> inputs, 
            IEnumerable<EntityId> outputs, IncrementId incId, CancellationToken ct);
        Task Flush();
    }
}