using System.Collections.Generic;
using Microsoft.IO;

namespace Funes.Sets {
    
    public class SetSnapshot : HashSet<string> {

        public SetSnapshot() {}
        
        public SetSnapshot(int capacity) : base(capacity){}

        public static readonly SetSnapshot Empty = new SetSnapshot();
    }
}