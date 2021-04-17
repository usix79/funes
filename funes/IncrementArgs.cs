using System;
using System.Collections.Generic;

namespace Funes {

    public interface IIncrementArgsCollector {
        void RegisterEntity(EntityStampKey key, bool asPremise);

        void RegisterEntry(EntityEntry entry, bool asPremise);
        
        void RegisterEvent(EntityId recordId, IncrementId first, IncrementId last);
    }
    public class IncrementArgs : IIncrementArgsCollector {
        
        public readonly struct InputEntityLink : IEquatable<InputEntityLink> {
            public bool Equals(InputEntityLink other) => Key.Equals(other.Key) && IsPremise == other.IsPremise;

            public override bool Equals(object? obj) => obj is InputEntityLink other && Equals(other);

            public override int GetHashCode() => HashCode.Combine(Key, IsPremise);

            public static bool operator ==(InputEntityLink left, InputEntityLink right) => left.Equals(right);

            public static bool operator !=(InputEntityLink left, InputEntityLink right) => !left.Equals(right);

            public InputEntityLink(EntityStampKey key, bool isPremise) {
                Key = key;
                IsPremise = isPremise;
            }
            public EntityStampKey Key { get; init; }
            public bool IsPremise { get; init; }
        }

        public readonly struct InputEventLink : IEquatable<InputEventLink> {
            public bool Equals(InputEventLink other) => RecordId.Equals(other.RecordId) && FirstIncId.Equals(other.FirstIncId) && LatIncId.Equals(other.LatIncId);

            public override bool Equals(object? obj) => obj is InputEventLink other && Equals(other);

            public override int GetHashCode() => HashCode.Combine(RecordId, FirstIncId, LatIncId);

            public static bool operator ==(InputEventLink left, InputEventLink right) => left.Equals(right);

            public static bool operator !=(InputEventLink left, InputEventLink right) => !left.Equals(right);

            public InputEventLink(EntityId recordId, IncrementId firstIncId, IncrementId latIncId) {
                RecordId = recordId;
                FirstIncId = firstIncId;
                LatIncId = latIncId;
            }

            public EntityId RecordId { get; init; }
            public IncrementId FirstIncId { get; init; }
            public IncrementId LatIncId { get; init; }
        }

        public List<InputEntityLink> Entities { get; init; } = new(); 

        public List<InputEventLink> Events { get; init; }= new();

        public int PremisesCount() {
            var premisesCount = 0;
            foreach (var link in Entities) {
                if (link.IsPremise) premisesCount++;
            }

            return premisesCount;
        }
        
        public void RegisterEntity(EntityStampKey key, bool asPremise) {
            var idx = -1;
            for (var i = 0; i < Entities.Count; i++) {
                if (Entities[i].Key == key) {
                    idx = i;
                    break;
                }
            }

            if (idx > 0) {
                Entities[idx] = new InputEntityLink(key, Entities[idx].IsPremise || asPremise);
            }
            else {
                Entities.Add(new InputEntityLink(key, asPremise));
            }
        }

        public void RegisterEntry(EntityEntry entry, bool asPremise) {
            if (entry.IsOk)
                RegisterEntity(entry.Key, asPremise);
        }

        public void RegisterEvent(EntityId recordId, IncrementId first, IncrementId last) {
            Events.Add(new InputEventLink(recordId, first, last));
        }
    }
}