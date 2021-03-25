using System;

namespace Funes {
    public readonly struct EntityId : IEquatable<EntityId> {
        public string Category { get; }
        public string Name { get; }

        public static EntityId None = new EntityId("", "");

        public EntityId(string cat, string name) => (Category, Name) = (cat, name);

        public bool Equals(EntityId other) => Category == other.Category && Name == other.Name;
        public override bool Equals(object? obj) => obj is EntityId other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Category, Name);
        public static bool operator ==(EntityId left, EntityId right) => left.Equals(right);
        public static bool operator !=(EntityId left, EntityId right) => !left.Equals(right);
        public override string ToString() => $"MemId {nameof(Category)}: {Category}, {nameof(Name)}: {Name}";
    }
}