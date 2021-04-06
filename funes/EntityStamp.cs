namespace Funes {
    public readonly struct EntityStamp {
        public Entity Entity { get; }
        public IncrementId IncId { get; }
        public EntityStampKey Key => new (Entity.Id, IncId);
        public object Value => Entity.Value;
        public EntityId EntId => Entity.Id;
        public EntityStamp(Entity entity, IncrementId incId) => (Entity, IncId) = (entity, incId);
        public EntityStamp(EntityStampKey key, object value) => (Entity, IncId) = (new Entity(key.EntId, value), key.IncId);
        
        public EntityEntry ToEntry() => EntityEntry.Ok(Entity, IncId);
    }
}