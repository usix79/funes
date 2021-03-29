using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataEngine : IDataSource {
        Task<Result<bool>> Upload(IEnumerable<EntityStamp> stamps, ISerializer serializer, CancellationToken ct, bool skipCache = false);
        
        Task<Result<bool>> Commit(IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct);
    }
}