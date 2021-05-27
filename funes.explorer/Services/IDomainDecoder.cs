namespace Funes.Explorer.Services {
    public interface IDomainDeserializer {
        public string Description { get; }
        Result<object> Deserialize(EntityId eid, BinaryData data);
    }
}