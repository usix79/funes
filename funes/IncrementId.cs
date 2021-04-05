using System;
using System.Diagnostics;
using System.Threading;

namespace Funes {
    public readonly struct IncrementId : IEquatable<IncrementId>, IComparable<IncrementId>, IComparable {
        public int CompareTo(object? obj) {
            if (ReferenceEquals(null, obj)) return 1;
            return obj is IncrementId other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(IncrementId)}");
        }
        
        public string Id { get; init; }

        public static readonly IncrementId Singularity = new ("");
        public static readonly IncrementId None = new ("");
        private static readonly DateTimeOffset FryReawakening = 
            new (3000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public IncrementId(string id) => Id = id;

        public IncrementId AsFail() => new (Id + FailTag);
        public IncrementId AsLost() => new (Id + LostTag);

        public bool IsSuccess() => !Id.EndsWith(FailTag) && !Id.EndsWith(LostTag);
        public bool IsNull() => Id is null;

        public static long MillisecondsBeforeFryReawakening(DateTimeOffset dt) => 
            Convert.ToInt64((FryReawakening - dt).TotalMilliseconds);

        private const int DigitsLength = 14;
        private const int TailLenght = 6;
        private const string FailTag = "-fail";
        private const string LostTag = "-lost";
        private const string FactTag = "-fact";

        private static readonly ThreadLocal<Random> Rand = new (() => new Random(DateTime.Now.Millisecond));
        
        public static IncrementId ComposeId(DateTimeOffset dt, Random? rand, bool forFact = false) {
            Debug.Assert(rand != null, nameof(rand) + " != null");

            var length = DigitsLength + 1 + TailLenght + (forFact ? FactTag.Length : 0);

            var id = string.Create(length, MillisecondsBeforeFryReawakening(dt), 
                (span, num) => {
                    for (var i = 0; i < DigitsLength; i++, num /= 10) {
                        span[DigitsLength - i - 1] = (char)('0' + num % 10);
                    }
                    span[DigitsLength] = '-';
                    for (var i = 0; i < TailLenght; i++) {
                        span[DigitsLength + i + 1] = (char) ('a' + rand.Next(25));
                    }
                    if (forFact) {
                        for (var i = 0; i < FactTag.Length; i++) {
                            span[ DigitsLength + 1 + TailLenght + i] = FactTag[i];
                        }
                    }
                });
            
            return new IncrementId(id);
        }

        public static IncrementId ComposeId(DateTimeOffset dt, string tail, bool forFact = false) {
            var length = DigitsLength + 1 + tail.Length + (forFact ? FactTag.Length : 0);
            var id = string.Create(length, MillisecondsBeforeFryReawakening(dt), 
                (span, num) => {
                    for (var i = 0; i < DigitsLength; i++, num /= 10) {
                        span[DigitsLength - i - 1] = (char)('0' + num % 10);
                    }
                    span[DigitsLength] = '-';
                    for (var i = 0; i < tail.Length; i++) {
                        span[DigitsLength + i + 1] = tail[i];
                    }
                    if (forFact) {
                        for (var i = 0; i < FactTag.Length; i++) {
                            span[ DigitsLength + 1 + tail.Length + i] = FactTag[i];
                        }
                    }
                });
            
            return new IncrementId(id);
        }

        public static IncrementId NewId() => ComposeId(DateTimeOffset.UtcNow, Rand.Value, false);
        public static IncrementId NewFactId() => ComposeId(DateTimeOffset.UtcNow, Rand.Value, true);

        public bool IsOlderThan(IncrementId other) => CompareTo(other) > 0;

        public bool IsNewerThan(IncrementId other) => CompareTo(other) < 0;

        public bool Equals(IncrementId other) => Id == other.Id;
        public override bool Equals(object? obj) => obj is IncrementId other && Equals(other);
        public override int GetHashCode() => Id.GetHashCode();
        public static bool operator ==(IncrementId left, IncrementId right) => left.Equals(right);
        public static bool operator !=(IncrementId left, IncrementId right) => !left.Equals(right);
        public int CompareTo(IncrementId other) => string.Compare(Id, other.Id, StringComparison.Ordinal);
        public override string ToString() => $"CID:{Id}";
    }
}