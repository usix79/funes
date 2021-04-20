using System;

namespace Funes {
    public readonly struct StampKey : IEquatable<StampKey> {
        public EntityId EntId { get; init; }
        public IncrementId IncId { get; init; }
        public StampKey(EntityId id, IncrementId incId) => (EntId, IncId) = (id, incId);
        
        public bool Equals(StampKey other) => EntId.Equals(other.EntId) && IncId.Equals(other.IncId);
        public override bool Equals(object? obj) => obj is StampKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(EntId, IncId);
        public static bool operator ==(StampKey left, StampKey right) => left.Equals(right);
        public static bool operator !=(StampKey left, StampKey right) => !left.Equals(right);
        public override string ToString() => $"{nameof(StampKey)} {nameof(EntId)}: {EntId}, {nameof(IncId)}: {IncId}";
    }
}