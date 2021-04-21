using System;

namespace Funes {
    
    public readonly struct EventLog {
        
        public ReadOnlyMemory<byte> Memory { get; }
   
        public IncrementId First { get; }

        public IncrementId Last { get; }

        public EventLog(IncrementId first, IncrementId last, ReadOnlyMemory<byte> data) =>
            (First, Last, Memory) = (first, last, data);

        public bool IsEmpty => Memory.Length == 0;

        public static EventLog Empty = new (IncrementId.None, IncrementId.None, ReadOnlyMemory<byte>.Empty);
    }
}