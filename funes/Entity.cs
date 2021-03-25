namespace Funes {
    public readonly struct Entity {

        public EntityId Id { get; }
        public object Value { get; }

        public Entity(EntityId id,  object value) {
            Id = id;
            Value = value;
        }
        
        // public interface ICache {
        //
        //     ValueTask<Result<bool>> Put(IEnumerable<MemStamp> mems, int ttl, IRepository.Encoder encoder);
        //
        //     ValueTask<Result<MemStamp>[]> Get(IEnumerable<(MemId, IRepository.Decoder)> ids);
        // }

        
    }
}