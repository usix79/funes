using System;

namespace Funes {
    public readonly struct MemId : IEquatable<MemId> {
        public string Category { get; }
        public string Name { get; }

        public static MemId None = new MemId("", "");

        public MemId(string cat, string name) => (Category, Name) = (cat, name);

        public bool Equals(MemId other) => Category == other.Category && Name == other.Name;
        public override bool Equals(object? obj) => obj is MemId other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Category, Name);
        public static bool operator ==(MemId left, MemId right) => left.Equals(right);
        public static bool operator !=(MemId left, MemId right) => !left.Equals(right);
        public override string ToString() => $"MemId {nameof(Category)}: {Category}, {nameof(Name)}: {Name}";
    }
}