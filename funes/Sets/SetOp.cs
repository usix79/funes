using System;

namespace Funes.Sets {
    
    public readonly struct SetOp : IEquatable<SetOp> {
        public enum Kind : byte { Unknown = 0, Add = 1, Del = 2, Clear = 3, ReplaceWith = 4, }

        public Kind OpKind { get; }
        public string Tag { get; }

        public SetOp(Kind kind, string tag) => (OpKind, Tag) = (kind, tag);
        
        public bool Equals(SetOp other) => OpKind == other.OpKind && Tag == other.Tag;

        public override bool Equals(object? obj) => obj is SetOp other && Equals(other);

        public override int GetHashCode() => HashCode.Combine((int) OpKind, Tag);

        public static bool operator ==(SetOp left, SetOp right) => left.Equals(right);

        public static bool operator !=(SetOp left, SetOp right) => !left.Equals(right);

        public override string ToString() => $"{nameof(OpKind)}: {OpKind}, {nameof(Tag)}: {Tag}";
    }
}