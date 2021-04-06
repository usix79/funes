using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface ITransactionEngine {
        Task<Result<Void>> TryCommit(IEnumerable<EntityStampKey> inputs,
            IEnumerable<EntityId> outputs, IncrementId incId, CancellationToken ct);
    }
}