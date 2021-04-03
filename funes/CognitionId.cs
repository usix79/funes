using System;
using System.Diagnostics;
using System.Threading;

namespace Funes {
    public readonly struct CognitionId : IEquatable<CognitionId>, IComparable<CognitionId>, IComparable {
        public int CompareTo(object? obj) {
            if (ReferenceEquals(null, obj)) return 1;
            return obj is CognitionId other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(CognitionId)}");
        }
        
        public string Id { get; init; }

        public static readonly CognitionId Singularity = new ("");
        public static readonly CognitionId None = new ("");
        private static readonly DateTimeOffset FryReawakening = 
            new (3000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public CognitionId(string id) => Id = id;

        public CognitionId AsFallacy() => new (Id + "-fallacy");
        public CognitionId AsLost() => new (Id + "-lost");

        public bool IsTruth() => !Id.EndsWith("-fallacy") && !Id.EndsWith("-lost");
        public bool IsNull() => Id is null;

        public static long MillisecondsBeforeFryReawakening(DateTimeOffset dt) => 
            Convert.ToInt64((FryReawakening - dt).TotalMilliseconds);

        private const int DigitsLength = 14;
        private const int TailLenght = 6;

        private static readonly ThreadLocal<Random> Rand = new (() => new Random(DateTime.Now.Millisecond));
        
        public static CognitionId ComposeId(DateTimeOffset dt, Random? rand) {
            Debug.Assert(rand != null, nameof(rand) + " != null");

            var id = string.Create(DigitsLength + 1 + TailLenght, MillisecondsBeforeFryReawakening(dt), 
                (span, num) => {
                    for (var i = 0; i < DigitsLength; i++, num /= 10) {
                        span[DigitsLength - i - 1] = (char)('0' + num % 10);
                    }
                    span[DigitsLength] = '-';
                    for (var i = 0; i < TailLenght; i++) {
                        span[DigitsLength + i + 1] = (char) ('a' + rand.Next(25));
                    }
                });
            
            return new CognitionId(id);
        }

        public static CognitionId ComposeId(DateTimeOffset dt, string tail) {
            var id = string.Create(DigitsLength + 1 + tail.Length, MillisecondsBeforeFryReawakening(dt), 
                (span, num) => {
                    for (var i = 0; i < DigitsLength; i++, num /= 10) {
                        span[DigitsLength - i - 1] = (char)('0' + num % 10);
                    }
                    span[DigitsLength] = '-';
                    for (var i = 0; i < tail.Length; i++) {
                        span[DigitsLength + i + 1] = tail[i];
                    }
                });
            
            return new CognitionId(id);
        }

        public static CognitionId NewId() => ComposeId(DateTimeOffset.UtcNow, Rand.Value);

        public bool IsOlderThan(CognitionId other) => CompareTo(other) > 0;

        public bool IsNewerThan(CognitionId other) => CompareTo(other) < 0;

        public bool Equals(CognitionId other) => Id == other.Id;
        public override bool Equals(object? obj) => obj is CognitionId other && Equals(other);
        public override int GetHashCode() => Id.GetHashCode();
        public static bool operator ==(CognitionId left, CognitionId right) => left.Equals(right);
        public static bool operator !=(CognitionId left, CognitionId right) => !left.Equals(right);
        public int CompareTo(CognitionId other) => string.Compare(Id, other.Id, StringComparison.Ordinal);
        public override string ToString() => $"CID:{Id}";
    }
}