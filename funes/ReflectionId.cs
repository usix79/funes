using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Funes {
    public readonly struct ReflectionId : IEquatable<ReflectionId> {
        public string Id { get; }

        public static readonly ReflectionId Singularity = new ("");

        public static readonly ReflectionId None = new ("");

        private static readonly DateTimeOffset FryReawakening = 
            new DateTimeOffset(3000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public ReflectionId(string id) => Id = id;
        
        public static long MillisecondsBeforeFryReawakening(DateTimeOffset dt) 
            => Convert.ToInt64((FryReawakening - dt).TotalMilliseconds);

        private const int DigitsLength = 14;
        private const int TailLenght = 6;

        private static readonly ThreadLocal<Random> Rand = new (() => new Random(DateTime.Now.Millisecond));
        
        public static ReflectionId ComposeId(DateTimeOffset dt, Random? rand) {
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
            
            return new ReflectionId(id);
        }

        public static ReflectionId NewId() 
            => ComposeId(DateTimeOffset.UtcNow, Rand.Value);
        
        public bool Equals(ReflectionId other) => Id == other.Id;

        public override bool Equals(object? obj) => obj is ReflectionId other && Equals(other);

        public override int GetHashCode() => Id.GetHashCode();

        public static bool operator ==(ReflectionId left, ReflectionId right) => left.Equals(right);

        public static bool operator !=(ReflectionId left, ReflectionId right) => !left.Equals(right);

        public override string ToString() => $"RID:{Id}";
    }
}