using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IRepository {
        ValueTask<Result<Void>> Save(EntityStamp stamp, ISerializer serializer, CancellationToken ct);
        ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer serializer, CancellationToken ct);
        ValueTask<Result<Void>> SaveBinary(EntityStampKey key, ReadOnlyMemory<byte> data, CancellationToken ct);
        ValueTask<Result<ReadOnlyMemory<byte>>> LoadBinary(EntityStampKey key, CancellationToken ct);
        ValueTask<Result<IncrementId[]>> HistoryBefore(EntityId eid, IncrementId before, int maxCount = 1, CancellationToken ct = default);
        ValueTask<Result<IncrementId[]>> HistoryAfter(EntityId eid, IncrementId after, CancellationToken ct = default);
    }
}