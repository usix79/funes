using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes {
    
    public interface IDataSource {

        public bool TryGetEntry(EntityId eid, bool asPremise, out EntityEntry entry);
        public Result<IReadOnlySet<string>> GetSet(string setName);
        
        ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct);
        
        ValueTask<Result<EventLog>> RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct);

        ValueTask<Result<IReadOnlySet<string>>> RetrieveSetSnapshot(string setName, CancellationToken ct);
    }
}