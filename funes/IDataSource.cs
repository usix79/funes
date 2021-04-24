using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes {
    
    public interface IDataSource {

        public bool TryGetEntry(EntityId eid, bool asPremise, out EntityEntry entry);
        public bool TryGetSet(string setName, out IReadOnlySet<string> set);
        
        ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct);
        
        ValueTask<Result<EventLog>> RetrieveEventLog(EntityId recordsId, EntityId offsetId, CancellationToken ct);

        ValueTask<Result<SetSnapshot>> RetrieveSetSnapshot(string setName, CancellationToken ct);
    }
}