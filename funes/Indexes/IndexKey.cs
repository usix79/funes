using System;
using System.Text;

namespace Funes.Indexes {
    
    public readonly struct IndexKey {
        
        public EntityId Id { get; }
        public BinaryData Data { get; }
        
        public IndexKey(EntityId id, BinaryData data) =>
            (Id, Data) = (id, data);
        
        public string GetValue() => IndexKeyHelpers.GetValue(Data);

        public BinaryStamp CreateStamp(IncrementId incId) =>
            new (Id.CreateStampKey(incId), Data);
    }

    public static class IndexKeyHelpers {
        public static IndexKey CreateKey(EntityId id, string value) {
            var memory = new Memory<byte>(new byte[value.Length * 2]);
            Encoding.Unicode.GetBytes(value, memory.Span);
            return new IndexKey(id, new BinaryData("bin", memory));
        }
        
        public static string GetValue(BinaryData data) =>
            Encoding.Unicode.GetString(data.Memory.Span);
    }
}