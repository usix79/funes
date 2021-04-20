using System.Collections.Generic;
using Funes.Indexes;
using Funes.Sets;

namespace Funes {
    public class LogicResult<TSideEffect> {
        public Dictionary<EntityId, Entity> Entities { get; } = new ();
        public Dictionary<string, SetRecord> SetRecords { get; } = new();
        public Dictionary<string, IndexRecord> IndexRecords { get; } = new();
        public List<TSideEffect> SideEffects { get; } = new ();
        public List<KeyValuePair<string, string>> Constants { get; } = new ();
    }
}