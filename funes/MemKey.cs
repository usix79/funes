using System;

namespace Funes {
    public readonly struct MemKey : IEquatable<MemKey>, IComparable<MemKey>, IComparable {
        public string Category { get; }
        public string Id { get; }

        public MemKey(string cat, string id) {
            Category = cat;
            Id = id;
        }

        public int CompareTo(MemKey other) {
            var categoryComparison = string.Compare(Category, other.Category, StringComparison.Ordinal);
            if (categoryComparison != 0) return categoryComparison;
            return string.Compare(Id, other.Id, StringComparison.Ordinal);
        }

        public int CompareTo(object? obj) {
            if (ReferenceEquals(null, obj)) return 1;
            return obj is MemKey other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(MemKey)}");
        }

        public bool Equals(MemKey other) {
            return Category == other.Category && Id == other.Id;
        }

        public override bool Equals(object? obj) {
            return obj is MemKey other && Equals(other);
        }

        public override int GetHashCode() {
            return HashCode.Combine(Category, Id);
        }

        public static bool operator ==(MemKey left, MemKey right) {
            return left.Equals(right);
        }

        public static bool operator !=(MemKey left, MemKey right) {
            return !left.Equals(right);
        }

        public override string ToString() {
            return $"MemKey {nameof(Category)}: {Category}, {nameof(Id)}: {Id}";
        }
    }
}