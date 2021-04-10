using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public delegate Task Behavior<in TSideEffect>(IncrementId incId, TSideEffect sideEffect, CancellationToken ct);
}