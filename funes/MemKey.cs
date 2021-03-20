using System.Threading;

namespace Funes {
    public readonly struct MemKey {
        public MemId Id { get; }
        public ReflectionId Rid { get; }
        public MemKey(MemId id, ReflectionId rid) => (Id, Rid) = (id, rid);
        public override string ToString() => $"MemKey {nameof(Id)}: {Id}, {nameof(Rid)}: {Rid}";
    }
}