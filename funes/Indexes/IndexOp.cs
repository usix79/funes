using System;

namespace Funes.Indexes {
    
    public readonly struct IndexOp : IEquatable<IndexOp> {
        public bool Equals(IndexOp other) => OpKind == other.OpKind && Key == other.Key && Value == other.Value;

        public override bool Equals(object? obj) => obj is IndexOp other && Equals(other);

        public override int GetHashCode() => HashCode.Combine((int) OpKind, Key, Value);

        public static bool operator ==(IndexOp left, IndexOp right) => left.Equals(right);

        public static bool operator !=(IndexOp left, IndexOp right) => !left.Equals(right);

        public IndexOp(Kind opKind, string key, string val) {
            OpKind = opKind;
            Key = key;
            Value = val;
        }

        public enum Kind : byte { Unknown = 0, Update = 1, Remove = 2}
     
        public Kind OpKind { get; }
        
        public string Key { get; }
        
        public string Value { get; }

        public override string ToString() => $"{nameof(OpKind)}: {OpKind}, {nameof(Key)}: {Key}, {nameof(Value)}: {Value}";
    }
}