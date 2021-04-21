using System;

namespace Funes.Indexes {
    
    public readonly struct IndexPage  {

        public ReadOnlyMemory<byte> Memory { get; }

        public IndexPage(ReadOnlyMemory<byte> memory) => Memory = memory;

        public PageKind Kind => PageKind.Unknown;

        public int Count => -1;

        public int CompareValue(int itemIdx, string value) {
            throw new NotImplementedException();
        }

        public int CompareKey(int itemIdx, string key) {
            throw new NotImplementedException();
        }
        
    }
}