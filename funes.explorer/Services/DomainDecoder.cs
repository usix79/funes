using System;
using System.Reflection;

namespace Funes.Explorer.Services {
    
    public class DomainDeserializer : IDomainDeserializer {

        private readonly ISerializer _ser;

        public DomainDeserializer(ISerializer ser) {
            _ser = ser;
        }

        public string Description => _ser.GetType().ToString();

        public Result<object> Deserialize(EntityId eid, BinaryData data) =>
            _ser.Decode(eid, data);
    }
}