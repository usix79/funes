namespace Funes {
    public struct EntityEntry {
        private enum Status { IsNotAvailable = 0, IsNotExist, IsOk }
        
        private Status _status;
        public Entity Entity { get; }
        
        public CognitionId Cid { get; }
        public bool IsNotAvailable => _status == Status.IsNotAvailable;
        public bool IsNotExist => _status == Status.IsNotExist;
        public bool IsOk => _status == Status.IsOk;
        public object Value => Entity.Value;
        public EntityId Eid => Entity.Id; 
        public EntityStampKey Key => new EntityStampKey(Entity.Id, Cid);
        public EntityStamp ToStamp() => new EntityStamp(Entity, Cid);

        public EntityEntry MapValue(object value) => new EntityEntry(new Entity(Entity.Id, value), Cid);

        private EntityEntry(Entity entity, CognitionId cid) => (Entity, Cid, _status) = (entity, cid, Status.IsOk);
        public static EntityEntry Ok(Entity entity) => new (entity, CognitionId.None);
        public static EntityEntry Ok(Entity entity, CognitionId cid) => new (entity, cid);
        public static readonly EntityEntry NotAvailable = new EntityEntry {_status = Status.IsNotAvailable};
        public static readonly EntityEntry NotExist = new EntityEntry {_status = Status.IsNotExist};
    }
}