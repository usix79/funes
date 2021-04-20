using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        Task<Result<Void>> Save(BinaryStamp stamp, CancellationToken ct);
        Task<Result<BinaryStamp>> Load(StampKey key, CancellationToken ct);
        Task<Result<IncrementId[]>> HistoryBefore(EntityId eid, IncrementId before, int maxCount = 1, CancellationToken ct = default);
        Task<Result<IncrementId[]>> HistoryAfter(EntityId eid, IncrementId after, CancellationToken ct = default);
    }
}