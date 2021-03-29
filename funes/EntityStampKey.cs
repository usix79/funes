using System;

namespace Funes {
    public readonly struct EntityStampKey : IEquatable<EntityStampKey> {
        public EntityId Eid { get; }
        public CognitionId Cid { get; }
        public EntityStampKey(EntityId id, CognitionId cid) => (Eid, Cid) = (id, cid);
        public bool Equals(EntityStampKey other) => Eid.Equals(other.Eid) && Cid.Equals(other.Cid);
        public override bool Equals(object? obj) => obj is EntityStampKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Eid, Cid);
        public static bool operator ==(EntityStampKey left, EntityStampKey right) => left.Equals(right);
        public static bool operator !=(EntityStampKey left, EntityStampKey right) => !left.Equals(right);
        public override string ToString() => $"{nameof(EntityStampKey)} {nameof(Eid)}: {Eid}, {nameof(Cid)}: {Cid}";
    }
}