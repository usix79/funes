using System;
using System.Data.Common;

namespace Funes {
    public readonly struct EntityId : IEquatable<EntityId> {
        
        public string Id { get; init; }

        public string GetCategory() {
            var idx = Id.LastIndexOf('/');
            return idx != -1 ? Id.Substring(0, idx) : "";
        }

        public string GetName() {
            var idx = Id.LastIndexOf('/');
            return idx != -1 ? Id.Substring(idx+1) : Id;
        }
        
        public static EntityId None = new EntityId("");
        
        public EntityId(string id) => Id = id;
        public EntityId(string cat, string name) => Id = cat + "/" + name;

        public StampKey CreateStampKey(IncrementId incId) => new (this, incId);

        public bool Equals(EntityId other) => Id == other.Id;
        public override bool Equals(object? obj) => obj is EntityId other && Equals(other);
        public override int GetHashCode() => Id.GetHashCode();
        public static bool operator ==(EntityId left, EntityId right) => left.Equals(right);
        public static bool operator !=(EntityId left, EntityId right) => !left.Equals(right);
        public override string ToString() => $"EntityId: {Id}";
    }
}