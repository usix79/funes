using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ITransactionEngine {
        Task<Result<Void>> TryCommit(ArraySegment<EntityStampKey> inputs,
            ArraySegment<EntityId> outputs, IncrementId incId, CancellationToken ct);
    }
}