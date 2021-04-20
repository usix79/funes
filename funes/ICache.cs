using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<BinaryStamp>> Get(EntityId eid, CancellationToken ct);
        Task<Result<Void>> Set(BinaryStamp stamp, CancellationToken ct);
        Task<Result<Void>> UpdateIfNewer(BinaryStamp stamp, CancellationToken ct);
        
        Task<Result<EventLog>> GetEventLog(EntityId eid, CancellationToken ct);
        Task<Result<Void>> UpdateEventsIfNotExists(EntityId eid, Event[] events, CancellationToken ct);
        Task<Result<int>> AppendEvent(EntityId eid, Event evt, CancellationToken ct);
        Task<Result<Void>> TruncateEvents(EntityId eid, IncrementId lastToTruncate, CancellationToken ct);
    }
}