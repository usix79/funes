using System;

namespace Funes {
    
    public readonly struct BinaryData : IEquatable<BinaryData> {
        
        public bool Equals(BinaryData other) => 
            Encoding == other.Encoding && Memory.Span.SequenceEqual(other.Memory.Span);

        public override bool Equals(object? obj) => obj is BinaryData other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(Encoding, Memory);

        public static bool operator ==(BinaryData left, BinaryData right) => left.Equals(right);

        public static bool operator !=(BinaryData left, BinaryData right) => !left.Equals(right);

        public BinaryData(string encoding, ReadOnlyMemory<byte> memory) {
            Encoding = encoding;
            Memory = memory;
        }

        public string Encoding { get; }
        
        public ReadOnlyMemory<byte> Memory { get; }

        public static readonly BinaryData Empty = new BinaryData("", ReadOnlyMemory<byte>.Empty);

    }
}