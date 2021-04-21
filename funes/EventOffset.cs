using System;
using System.Buffers.Binary;
using System.Text;

namespace Funes {
    
    public readonly struct EventOffset {
        
        public BinaryData Data { get; }

        public EventOffset(BinaryData data) => Data = data;
        
        public long Gen => Data.IsEmpty ? 0 : BinaryPrimitives.ReadInt64LittleEndian(Data.Memory.Span);
        public IncrementId GetLastIncId() =>
            Data.IsEmpty
                ? IncrementId.None
                : new IncrementId(Encoding.Unicode.GetString(Data.Memory.Slice(8).Span));

        public EventOffset NextGen(IncrementId eventLogLast) {
            var arr = new byte[8 + eventLogLast.Id.Length * 2];
            var memory = new Memory<byte>(arr);
            BinaryPrimitives.WriteInt64LittleEndian(memory.Span, Gen + 1);
            Encoding.Unicode.GetBytes(eventLogLast.Id, memory.Slice(8).Span);

            return new EventOffset(new BinaryData("bin", memory));
        }

        public BinaryStamp CreateStamp(EntityId offsetId, IncrementId incId) =>
            new (offsetId.CreateStampKey(incId), Data);
        
    }
}