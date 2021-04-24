using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    public class ContextDataEngine : IDataEngine {
        
        private readonly IDataEngine _dataEngine;

        public ContextDataEngine(IDataEngine dataEngine) => _dataEngine = dataEngine;

        public ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct) => throw new NotImplementedException();

        public ValueTask<Result<EventLog>> RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct) => throw new NotImplementedException();

        public ValueTask<Result<Void>> Upload(BinaryStamp stamp, CancellationToken ct, bool skipCache = false) => throw new NotImplementedException();

        public ValueTask<Result<int>> AppendEvent(EntityId recordId, Event evt, EntityId offsetId, CancellationToken ct, bool skipCache = false) => throw new NotImplementedException();

        public ValueTask<Result<Void>> TruncateEvents(EntityId recordId, IncrementId lastToTruncate, CancellationToken ct) => throw new NotImplementedException();

        public ValueTask<Result<Void>> TryCommit(ArraySegment<StampKey> premises, ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct) => throw new NotImplementedException();

        public ValueTask Flush(CancellationToken ct = default) => throw new NotImplementedException();
    }
}