using System;
using System.Text;

namespace Funes.Indexes {
    
    public readonly struct IndexKey {
        
        public EntityId Id { get; }
        public BinaryData Data { get; }
        
        public IndexKey(EntityId id, BinaryData data) =>
            (Id, Data) = (id, data);
        
        public string GetValue() => Encoding.Unicode.GetString(Data.Memory.Span);

        public BinaryStamp CreateStamp(IncrementId incId) =>
            new (Id.CreateStampKey(incId), Data);
    }

    public static class IndexKeyHelpers {
        public static IndexKey CreateKey(EntityId id, string value) {
            var memory = new Memory<byte>(new byte[value.Length * 2]);
            Encoding.Unicode.GetBytes(value, memory.Span);
            return new IndexKey(id, new BinaryData("bin", memory));
        }
    }
}