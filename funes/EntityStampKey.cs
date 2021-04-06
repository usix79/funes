using System;

namespace Funes {
    public readonly struct EntityStampKey : IEquatable<EntityStampKey> {
        public EntityId EntId { get; init; }
        public IncrementId IncId { get; init; }
        public EntityStampKey(EntityId id, IncrementId incId) => (EntId, IncId) = (id, incId);
        
        public bool IsNull => EntId.Id is null;

        public bool Equals(EntityStampKey other) => EntId.Equals(other.EntId) && IncId.Equals(other.IncId);
        public override bool Equals(object? obj) => obj is EntityStampKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(EntId, IncId);
        public static bool operator ==(EntityStampKey left, EntityStampKey right) => left.Equals(right);
        public static bool operator !=(EntityStampKey left, EntityStampKey right) => !left.Equals(right);
        public override string ToString() => $"{nameof(EntityStampKey)} {nameof(EntId)}: {EntId}, {nameof(IncId)}: {IncId}";
    }
}