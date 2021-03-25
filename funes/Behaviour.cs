using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public delegate Task Behaviour<in TSideEffect>(TSideEffect sideEffect, CancellationToken ct);
}