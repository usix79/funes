using System;

namespace Funes {
    
    public readonly struct Event {
        public ReadOnlyMemory<byte> Data { get; }
        public IncrementId IncId { get; }

        public Event(IncrementId incId, ReadOnlyMemory<byte> data) =>
            (IncId, Data) = (incId, data);
    }
}