namespace Funes {
    public struct EntityEntry {
        private enum Status { IsNotAvailable = 0, IsNotExist, IsOk }
        
        private Status _status;
        public Entity Entity { get; }
        public bool IsNotAvailable => _status == Status.IsNotAvailable;
        public bool IsNotExist => _status == Status.IsNotExist;
        public bool IsOk => _status == Status.IsOk;
        public object Value => Entity.Value;

        private EntityEntry(Entity entity) => (Entity, _status) = (entity, Status.IsOk);
        public static EntityEntry Ok(Entity entity) => new (entity);
        public static readonly EntityEntry NotAvailable = new EntityEntry {_status = Status.IsNotAvailable};
        public static readonly EntityEntry NotExist = new EntityEntry {_status = Status.IsNotExist};
    }
}