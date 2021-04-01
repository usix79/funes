using System;

namespace Funes {
    public readonly struct Entity : IEquatable<Entity> {
        public EntityId Id { get; }
        public object Value { get; }
        public Entity(EntityId id, object value) => (Id, Value) = (id, value);
        
        public Entity MapValue(object value) => new (Id, value);
        
        public bool Equals(Entity other) => Id.Equals(other.Id) && Value.Equals(other.Value);
        public override bool Equals(object? obj) => obj is Entity other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Id, Value);
        public static bool operator ==(Entity left, Entity right) => left.Equals(right);
        public static bool operator !=(Entity left, Entity right) => !left.Equals(right);

        public override string ToString() => $"{Id}::{Value}";
    }
}