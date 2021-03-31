using System;

namespace Funes {
    public readonly struct EntityEntry : IEquatable<EntityEntry> {
        public enum EntryStatus { IsNotAvailable = 0, IsNotExist, IsOk }
        
        public EntryStatus Status { get; init; }
        
        public EntityId Eid { get; init; }
        public object Value { get; }
        public CognitionId Cid { get; }
        
        public bool IsNotAvailable => Status == EntryStatus.IsNotAvailable;
        public bool IsNotExist => Status == EntryStatus.IsNotExist;
        public bool IsOk => Status == EntryStatus.IsOk;

        public Entity Entity=> new (Eid, Value);
        public EntityStampKey Key => new (Eid, Cid);
        public EntityStamp ToStamp() => new (Entity, Cid);
        
        public EntityEntry MapValue(object value) => new (new Entity(Eid, value), Cid);

        private EntityEntry(Entity entity, CognitionId cid) => 
            (Eid, Value, Cid, Status) = (entity.Id, entity.Value, cid, EntryStatus.IsOk);
        public static EntityEntry Ok(Entity entity) => new (entity, CognitionId.None);
        public static EntityEntry Ok(Entity entity, CognitionId cid) => new (entity, cid);
        public static EntityEntry NotAvailable(EntityId eid) => 
            new EntityEntry {Eid = eid, Status = EntryStatus.IsNotAvailable};
        public static EntityEntry NotExist(EntityId eid) => 
            new  EntityEntry {Eid = eid, Status = EntryStatus.IsNotExist};
        
        public bool Equals(EntityEntry other) 
            => Status == other.Status && Equals(Value, other.Value) && Cid.Equals(other.Cid);
        public override bool Equals(object? obj) => obj is EntityEntry other && Equals(other);
        public override int GetHashCode() => HashCode.Combine((int) Status, Value, Cid);
        public static bool operator ==(EntityEntry left, EntityEntry right) => left.Equals(right);
        public static bool operator !=(EntityEntry left, EntityEntry right) => !left.Equals(right);
    }
}