using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {

    public class SimpleTransactionEngine: ITransactionEngine {
        private readonly Dictionary<EntityId, IncrementId> _actualIncIds = new();
        public Task<Result<Void>> TryCommit(
            ArraySegment<EntityStampKey> premises, 
            ArraySegment<EntityId> conclusions, IncrementId incId, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            lock (_actualIncIds) {
                List<Error.CommitError.Conflict>? conflicts = null;
                foreach(var premise in premises){
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
                    return Task.FromResult(Result<Void>.CommitError(conflicts.ToArray()));
                }

                foreach (var entId in conclusions) {
                    _actualIncIds[entId] = incId;
                }
            }

            return Task.FromResult(new Result<Void>(Void.Value));
        }
    }
}