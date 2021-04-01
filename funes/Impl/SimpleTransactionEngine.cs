using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {

    public class SimpleTransactionEngine: ITransactionEngine {
        private readonly Dictionary<EntityId, CognitionId> _actualCids = new();
        public Task<Result<bool>> Commit(
                IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions, CancellationToken ct) {
            ct.ThrowIfCancellationRequested();

            lock (_actualCids) {
                List<Error.TransactionError.Conflict>? conflicts = null;
                foreach (var premise in premises) {
                    if (_actualCids.TryGetValue(premise.Eid, out var actualCid)) {
                        if (actualCid != premise.Cid) {
                            conflicts ??= new List<Error.TransactionError.Conflict>();
                            conflicts.Add(new Error.TransactionError.Conflict {
                                Eid = premise.Eid, PremiseCid = premise.Cid, ActualCid = actualCid
                            });
                        }
                    }
                }

                if (conflicts is not null) {
                    return Task.FromResult(Result<bool>.TransactionError(conflicts.ToArray()));
                }

                foreach (var conclusion in conclusions) {
                    _actualCids[conclusion.Eid] = conclusion.Cid;
                }
            }

            return Task.FromResult(new Result<bool>(true));
        }
    }
}