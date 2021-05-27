using System;
using System.Reflection.Metadata.Ecma335;

namespace Funes.Explorer.Services {
    public class DummyDeserializer : IDomainDeserializer {
        public string Description => "Dummy";

        public Result<object> Deserialize(EntityId eid, BinaryData data) =>
            new (BitConverter.ToString(data.Memory.ToArray()));
    }
}