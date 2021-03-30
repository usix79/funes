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
                foreach (var premise in premises) {
                    if (_actualCids.TryGetValue(premise.Eid, out var actualCid)) {
                        if (actualCid != premise.Cid) return Task.FromResult(new Result<bool>(false));
                    }
                }

                foreach (var conclusion in conclusions) {
                    _actualCids[conclusion.Eid] = conclusion.Cid;
                }
            }

            return Task.FromResult(new Result<bool>(true));
        }
    }
}