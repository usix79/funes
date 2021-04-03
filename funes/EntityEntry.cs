using System;

namespace Funes {
    public readonly struct EntityEntry : IEquatable<EntityEntry> {
        public enum EntryStatus { IsNotAvailable = 0, IsNotExist, IsOk }
        
        public EntryStatus Status { get; init; }
        
        public EntityId EntId { get; init; }
        public object Value { get; }
        public IncrementId IncId { get; }
        
        public bool IsNotAvailable => Status == EntryStatus.IsNotAvailable;
        public bool IsNotExist => Status == EntryStatus.IsNotExist;
        public bool IsOk => Status == EntryStatus.IsOk;

        public Entity Entity=> new (EntId, Value);
        public EntityStampKey Key => new (EntId, IncId);
        public EntityStamp ToStamp() => new (Entity, IncId);
        
        public EntityEntry MapValue(object value) => new (new Entity(EntId, value), IncId);

        private EntityEntry(Entity entity, IncrementId incId) => 
            (EntId, Value, IncId, Status) = (entity.Id, entity.Value, incId, EntryStatus.IsOk);
        public static EntityEntry Ok(Entity entity) => new (entity, IncrementId.None);
        public static EntityEntry Ok(Entity entity, IncrementId incId) => new (entity, incId);
        public static EntityEntry NotAvailable(EntityId eid) => 
            new EntityEntry {EntId = eid, Status = EntryStatus.IsNotAvailable};
        public static EntityEntry NotExist(EntityId eid) => 
            new  EntityEntry {EntId = eid, Status = EntryStatus.IsNotExist};
        
        public bool Equals(EntityEntry other) 
            => Status == other.Status && Equals(Value, other.Value) && IncId.Equals(other.IncId);
        public override bool Equals(object? obj) => obj is EntityEntry other && Equals(other);
        public override int GetHashCode() => HashCode.Combine((int) Status, Value, IncId);
        public static bool operator ==(EntityEntry left, EntityEntry right) => left.Equals(right);
        public static bool operator !=(EntityEntry left, EntityEntry right) => !left.Equals(right);

        public override string ToString() {
            return $"{nameof(Status)}: {Status}, {nameof(EntId)}: {EntId}, {nameof(Value)}: {Value}, {nameof(IncId)}: {IncId}";
        }
    }
}