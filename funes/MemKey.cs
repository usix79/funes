using System;
using System.Threading;

namespace Funes {
    public readonly struct MemKey : IEquatable<MemKey> {
        public MemId Id { get; }
        public ReflectionId Rid { get; }
        public MemKey(MemId id, ReflectionId rid) => (Id, Rid) = (id, rid);
        public bool Equals(MemKey other) => Id.Equals(other.Id) && Rid.Equals(other.Rid);
        public override bool Equals(object? obj) => obj is MemKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Id, Rid);
        public static bool operator ==(MemKey left, MemKey right) => left.Equals(right);
        public static bool operator !=(MemKey left, MemKey right) => !left.Equals(right);
        public override string ToString() => $"MemKey {nameof(Id)}: {Id}, {nameof(Rid)}: {Rid}";
    }
}