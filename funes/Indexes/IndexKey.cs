using System.Text;

namespace Funes.Indexes {
    
    public readonly struct IndexKey {
        
        public IndexKey(BinaryData data) =>
            Data = data;

        public BinaryData Data { get; }

        public string GetValue() =>
            Data.Memory.IsEmpty ? "" : Encoding.Unicode.GetString(Data.Memory.Span);
    }
}