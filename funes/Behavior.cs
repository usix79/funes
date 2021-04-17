using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public delegate ValueTask<Result<Void>> Behavior<in TSideEffect>(
        IncrementId incId, TSideEffect sideEffect, CancellationToken ct);
}