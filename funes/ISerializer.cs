
namespace Funes {
    public interface ISerializer {
        public Result<BinaryData> Encode(EntityId eid, object content);
        public Result<object> Decode(EntityId eid, BinaryData data);
    }
}