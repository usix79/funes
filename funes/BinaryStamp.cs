using System;

namespace Funes {
    public readonly struct BinaryStamp : IEquatable<BinaryStamp> {
        public bool Equals(BinaryStamp other) => Eid.Equals(other.Eid) && IncId.Equals(other.IncId) && Data.Equals(other.Data);

        public override bool Equals(object? obj) => obj is BinaryStamp other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(Eid, IncId, Data);

        public static bool operator ==(BinaryStamp left, BinaryStamp right) => left.Equals(right);

        public static bool operator !=(BinaryStamp left, BinaryStamp right) => !left.Equals(right);

        public BinaryStamp(StampKey key, BinaryData data) {
            Eid = key.EntId;
            IncId = key.IncId;
            Data = data;
        }

        private BinaryStamp(EntityId eid) {
            Eid = eid;
            IncId = IncrementId.None;
            Data = BinaryData.Empty;
        }

        public EntityId Eid { get; }

        public IncrementId IncId { get;}
        
        public BinaryData Data { get; }

        public StampKey Key => new (Eid, IncId);
        
        public bool IsEmpty => IncId == IncrementId.None;

        public bool IsNotEmpty => !IsEmpty;

        public static BinaryStamp Empty(EntityId eid) => new (eid);

    }
}