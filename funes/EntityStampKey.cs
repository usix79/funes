using System;

namespace Funes {
    public readonly struct EntityStampKey : IEquatable<EntityStampKey> {
        public EntityId Eid { get; }
        public ReflectionId Rid { get; }
        public EntityStampKey(EntityId id, ReflectionId rid) => (Eid, Rid) = (id, rid);
        public bool Equals(EntityStampKey other) => Eid.Equals(other.Eid) && Rid.Equals(other.Rid);
        public override bool Equals(object? obj) => obj is EntityStampKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Eid, Rid);
        public static bool operator ==(EntityStampKey left, EntityStampKey right) => left.Equals(right);
        public static bool operator !=(EntityStampKey left, EntityStampKey right) => !left.Equals(right);
        public override string ToString() => $"MemKey {nameof(Eid)}: {Eid}, {nameof(Rid)}: {Rid}";
    }
}