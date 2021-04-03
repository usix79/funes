using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        ValueTask<Result<bool>> Upload(IEnumerable<EntityStamp> stamps, ISerializer ser, CancellationToken ct, bool skipCache = false);
        ValueTask<Result<Void>> TryCommit(IEnumerable<EntityStampKey> inputs, IEnumerable<EntityStampKey> outputs, CancellationToken ct);
        Task Flush();
    }
}