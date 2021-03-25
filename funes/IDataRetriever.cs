using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public interface IDataRetriever {
        ValueTask<Result<EntityStamp>> Retrieve(EntityId eid, CancellationToken ct);
    }
}