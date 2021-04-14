using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ICache {

        Task<Result<EntityEntry>> Get(EntityId eid, ISerializer ser, CancellationToken ct);
        Task<Result<Void>> Set(EntityEntry entry, ISerializer ser, CancellationToken ct);
        Task<Result<Void>> UpdateIfNewer(EntityEntry entry, ISerializer ser, CancellationToken ct);
        
        Task<Result<EventLog>> GetEvents(EntityId eid, CancellationToken ct);
        Task<Result<Void>> UpdateEventsIfNotExists(EntityId eid, Event[] events, CancellationToken ct);
        Task<Result<int>> AppendEvent(EntityId eid, Event evt, CancellationToken ct);
        Task<Result<Void>> TruncateEvents(EntityId eid, IncrementId lastToTruncate, CancellationToken ct);
    }
}