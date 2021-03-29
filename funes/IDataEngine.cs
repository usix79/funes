using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource, ISourceOfTruth {
        Task<Result<bool>> Upload(IEnumerable<EntityStamp> mems, ISerializer serializer, CancellationToken ct, bool skipCache = false);
    }
}