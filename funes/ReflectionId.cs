using System;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace Funes {
    public struct ReflectionId : IEquatable<ReflectionId>, IComparable<ReflectionId>, IComparable {
        public string Id { get; init; }

        public static readonly ReflectionId Empty = new ReflectionId {Id = ""};

        private static readonly DateTimeOffset FryReawakening = new DateTimeOffset(3000, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public static long MillisecondsBeforeFryReawakening(DateTimeOffset dt) 
            => Convert.ToInt64((FryReawakening - dt).TotalMilliseconds);

        private const int TailLenght = 6;

        private static readonly ThreadLocal<Random> Rand = new ThreadLocal<Random>(
            () => new Random(DateTime.Now.Millisecond));
        
        public static ReflectionId ComposeId(DateTimeOffset dt, Random? rand) {
            Debug.Assert(rand != null, nameof(rand) + " != null");

            var txt = new StringBuilder(64);
            AppendDigits(MillisecondsBeforeFryReawakening(dt));
            txt.Append('-');
            for (var i = 0; i < TailLenght; i++) {
                txt.Append((char)('a' + rand.Next(25)));
            }
            return new ReflectionId {Id = txt.ToString()};
            
            void AppendDigits(long num) {
                if (num > 9) {
                    AppendDigits(num / 10);
                } 
                txt.Append((char)('0' + (num % 10)));
            }
        }

        public static ReflectionId NewId() =>
            ComposeId(DateTimeOffset.UtcNow, Rand.Value);
        
        public int CompareTo(ReflectionId other) {
            return string.Compare(Id, other.Id, StringComparison.Ordinal);
        }

        public int CompareTo(object? obj) {
            if (ReferenceEquals(null, obj)) return 1;
            return obj is ReflectionId other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(ReflectionId)}");
        }

        public bool Equals(ReflectionId other) {
            return Id == other.Id;
        }

        public override bool Equals(object? obj) {
            return obj is ReflectionId other && Equals(other);
        }

        public override int GetHashCode() {
            return Id.GetHashCode();
        }

        public static bool operator ==(ReflectionId left, ReflectionId right) {
            return left.Equals(right);
        }

        public static bool operator !=(ReflectionId left, ReflectionId right) {
            return !left.Equals(right);
        }

        public override string ToString() {
            return $"RID:{Id}";
        }
    }
}