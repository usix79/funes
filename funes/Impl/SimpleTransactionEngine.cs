using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {

    public class SimpleTransactionEngine: ITransactionEngine {
        private readonly Dictionary<EntityId, IncrementId> _actualIncIds = new();
        public Task<Result<Void>> TryCommit(
                IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            lock (_actualIncIds) {
                List<Error.CommitError.Conflict>? conflicts = null;
                foreach (var premise in premises) {
                    if (_actualIncIds.TryGetValue(premise.EntId, out var actualIncId)) {
                        if (actualIncId != premise.IncId) {
                            conflicts ??= new List<Error.CommitError.Conflict>();
                            conflicts.Add(new Error.CommitError.Conflict {
                                EntId = premise.EntId, PremiseIncId = premise.IncId, ActualIncId = actualIncId
                            });
                        }
                    }
                }

                if (conflicts is not null) {
                    return Task.FromResult(Result<Void>.TransactionError(conflicts.ToArray()));
                }

                foreach (var conclusion in conclusions) {
                    _actualIncIds[conclusion.EntId] = conclusion.IncId;
                }
            }

            return Task.FromResult(new Result<Void>(Void.Value));
        }
    }
}