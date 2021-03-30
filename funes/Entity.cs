namespace Funes {
    public readonly struct Entity {
        public EntityId Id { get; }
        public object Value { get; }
        public Entity(EntityId id, object value) => (Id, Value) = (id, value);
    }
}