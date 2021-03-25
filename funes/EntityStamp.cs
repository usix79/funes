namespace Funes {
    public readonly struct EntityStamp {
        public Entity Entity { get; }
        public ReflectionId Rid { get; }
        public EntityStampKey Key => new (Entity.Id, Rid);
        public object Value => Entity.Value;
        public EntityStamp(Entity entity, ReflectionId rid) => (Entity, Rid) = (entity, rid);
        public EntityStamp(EntityStampKey key, object value) => (Entity, Rid) = (new Entity(key.Eid, value), key.Rid);
    }
}