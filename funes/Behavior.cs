using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public delegate Task Behavior<in TSideEffect>(TSideEffect sideEffect, CancellationToken ct);
}