namespace Funes {
    public readonly struct EntityStamp {
        public Entity Entity { get; }
        public CognitionId Cid { get; }
        public EntityStampKey Key => new (Entity.Id, Cid);
        public object Value => Entity.Value;
        public EntityStamp(Entity entity, CognitionId cid) => (Entity, Cid) = (entity, cid);
        public EntityStamp(EntityStampKey key, object value) => (Entity, Cid) = (new Entity(key.Eid, value), key.Cid);
        
        public EntityEntry ToEntry() => EntityEntry.Ok(Entity, Cid);
    }
}