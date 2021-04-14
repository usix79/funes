using System;
using Microsoft.VisualBasic.CompilerServices;

namespace Funes.Indexes {
    
    public readonly struct IndexOp : IEquatable<IndexOp> {
        public enum Kind : byte { Unknown = 0, AddTag = 1, DelTag = 2, ClearTags = 3, ReplaceTags = 4, }

        public Kind OpKind { get; }
        public string Key { get; }
        public string Tag { get; }

        public IndexOp(Kind kind, string key, string tag) => (OpKind, Key, Tag) = (kind, key, tag);
        
        public bool Equals(IndexOp other) => OpKind == other.OpKind && Key == other.Key && Tag == other.Tag;

        public override bool Equals(object? obj) => obj is IndexOp other && Equals(other);

        public override int GetHashCode() => HashCode.Combine((int) OpKind, Key, Tag);

        public static bool operator ==(IndexOp left, IndexOp right) => left.Equals(right);

        public static bool operator !=(IndexOp left, IndexOp right) => !left.Equals(right);

        public override string ToString() => $"{nameof(OpKind)}: {OpKind}, {nameof(Key)}: {Key}, {nameof(Tag)}: {Tag}";
    }
}