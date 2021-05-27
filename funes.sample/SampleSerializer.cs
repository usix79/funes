using Funes;
using Funes.Impl;
using funes.sample.Domain;

namespace funes.sample {
    public class SampleSerializer : ISerializer {
        private readonly ISerializer _bookSer = new SimpleSerializer<Book>();
        private readonly ISerializer _popSer = new SimpleSerializer<Operation.PopulateSampleData>();
        private readonly ISerializer _likeSer = new SimpleSerializer<Operation.Like>();
        public Result<BinaryData> Encode(EntityId eid, object content) => 
            eid.GetCategory() switch {
                Helpers.Constants.CatBooks => _bookSer.Encode(eid, content),        
                Helpers.Constants.CatOperations => 
                    eid.GetName() switch {
                        Helpers.Constants.OperationPopulate => _popSer.Encode(eid, content),        
                        Helpers.Constants.OperationLike => _likeSer.Encode(eid, content),        
                        _ => Result<BinaryData>.SerdeError($"Not Supported Operation {eid.GetName()}")
                    },
                _ => Result<BinaryData>.SerdeError($"Not Supported Entity {eid.Id}")
            };

        public Result<object> Decode(EntityId eid, BinaryData data) => 
            eid.GetCategory() switch {
                Helpers.Constants.CatBooks => _bookSer.Decode(eid, data),        
                Helpers.Constants.CatOperations => 
                    eid.GetName() switch {
                        Helpers.Constants.OperationPopulate => _popSer.Decode(eid, data),        
                        Helpers.Constants.OperationLike => _likeSer.Decode(eid, data),        
                        _ => Result<object>.SerdeError($"Not Supported Operation {eid.GetName()}")
                    },
                _ => Result<object>.SerdeError($"Not Supported Entity {eid.Id}")
            };
    }
}