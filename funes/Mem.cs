using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Funes {
    public readonly struct Mem {

        public MemId Id { get; }
        public object Value { get; }

        public Mem(MemId id,  object value) {
            Id = id;
            Value = value;
        }
        
        // public interface ICache {
        //
        //     ValueTask<Result<bool>> Put(IEnumerable<MemStamp> mems, int ttl, IRepository.Encoder encoder);
        //
        //     ValueTask<Result<MemStamp>[]> Get(IEnumerable<(MemId, IRepository.Decoder)> ids);
        // }

        public interface IRepository {
            public delegate ValueTask<Result<string>> Encoder(Stream output, object content);
            public delegate ValueTask<Result<object>> Decoder(Stream input, string encoding);

            ValueTask<Result<bool>> Put(MemStamp memStamp, Encoder encoder);
            ValueTask<Result<MemStamp>> Get(MemKey key, Decoder decoder);
            ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1);


            // TODO: future approach for reducing allocations
            // ValueTask<Result<bool>> Put(MemKey key, ArraySegment<byte> buffer, string encoding);
            // async ValueTask<Result<bool>> Put(Mem mem, Encoder encoder) {
            //     await using MemoryStream stream = new();
            //     var result = await encoder(stream, mem.Value);
            //     if (result.IsOk) {
            //         return await Put(mem.Key, stream.GetBuffer(), result.Value);
            //     }
            //
            //     return new Result<bool>(result.Error);
            // }
        }
        
    }

    public readonly struct MemStamp {
        
        public Mem Mem { get; }
        
        public ReflectionId Rid { get; }

        public MemKey Key => new MemKey(Mem.Id, Rid);
        public object Value => Mem.Value;

        public MemStamp(Mem mem, ReflectionId rid) => (Mem, Rid) = (mem, rid);
        public MemStamp(MemKey key, object value) => (Mem, Rid) = (new Mem(key.Id, value), key.Rid);
    }

    public readonly struct MemId : IEquatable<MemId> {
        public string Category { get; }
        public string Name { get; }

        public static MemId None = new MemId("", "");

        public MemId(string cat, string name) => (Category, Name) = (cat, name);

        public bool Equals(MemId other) => Category == other.Category && Name == other.Name;
        public override bool Equals(object? obj) => obj is MemId other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Category, Name);
        public static bool operator ==(MemId left, MemId right) => left.Equals(right);
        public static bool operator !=(MemId left, MemId right) => !left.Equals(right);
        public override string ToString() => $"MemId {nameof(Category)}: {Category}, {nameof(Name)}: {Name}";
    }
    
    public readonly struct MemKey : IEquatable<MemKey> {
        public MemId Id { get; }
        public ReflectionId Rid { get; }
        public MemKey(MemId id, ReflectionId rid) => (Id, Rid) = (id, rid);
        public bool Equals(MemKey other) => Id.Equals(other.Id) && Rid.Equals(other.Rid);
        public override bool Equals(object? obj) => obj is MemKey other && Equals(other);
        public override int GetHashCode() => HashCode.Combine(Id, Rid);
        public static bool operator ==(MemKey left, MemKey right) => left.Equals(right);
        public static bool operator !=(MemKey left, MemKey right) => !left.Equals(right);
        public override string ToString() => $"MemKey {nameof(Id)}: {Id}, {nameof(Rid)}: {Rid}";
    }
}