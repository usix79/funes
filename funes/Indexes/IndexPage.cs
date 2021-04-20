using System;

namespace Funes.Indexes {
    
    public class IndexPage : IEquatable<IndexPage> {
        public bool Equals(IndexPage? other) {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return _data.Span.SequenceEqual(other._data.Span);
        }

        public override bool Equals(object? obj) {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((IndexPage) obj);
        }

        public override int GetHashCode() => _data.GetHashCode();

        public static bool operator ==(IndexPage? left, IndexPage? right) => Equals(left, right);

        public static bool operator !=(IndexPage? left, IndexPage? right) => !Equals(left, right);

        private readonly ReadOnlyMemory<byte> _data;

        public IndexPage(ReadOnlyMemory<byte> data) => _data = data;

        public PageKind Kind { get; }

        public int Count { get; }

        public int CompareValue(int itemIdx, string value) {
            throw new NotImplementedException();
        }

        public int CompareKey(int itemIdx, string key) {
            throw new NotImplementedException();
        }
        
    }
}