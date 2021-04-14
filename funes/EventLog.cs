using System;

namespace Funes {
    
    public readonly struct EventLog {
        
        public ReadOnlyMemory<byte> Data { get; }
   
        public IncrementId First { get; }

        public IncrementId Last { get; }

        public EventLog(IncrementId first, IncrementId last, ReadOnlyMemory<byte> data) =>
            (First, Last, Data) = (first, last, data);

        public bool IsEmpty => Data.Length == 0;
    }
}