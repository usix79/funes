using System;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;

namespace Funes.Tests {
    public static class TestHelpers {
        
        private static readonly Random Rand = new Random(DateTime.Now.Millisecond);
        private static readonly ISerializer SimpleSerializer = new SimpleSerializer<Simple>();

        private static string RandomString(int length) =>
            string.Create(length, length, (span, n) => {
                for (var i = 0; i < n; i++) {
                    span[i] = (char) ('a' + Rand.Next(25));
                }
            });
        
        public static EntityId CreateRandomEntId(string? cat = null) =>
            new (cat ?? "cat-" + RandomString(10), "id-" + RandomString(10));

        public static IncrementId CreateRandomIncId(string? cat = null) => IncrementId.NewId();

        public static EntityStampKey CreateRandomStampKey() => CreateRandomEntId().CreateStampKey(CreateRandomIncId());

        public static Simple CreateRandomValue() =>
            new (Rand.Next(1024), RandomString(1024));    

        public static EntityStamp CreateSimpleEntityStamp(IncrementId incId, EntityId? eid = null) =>
            new EntityStamp (new Entity(eid??CreateRandomEntId(), CreateRandomValue()), incId);
        
        public static void AssertEntitiesEqual(EntityStamp expected, EntityStamp actual) {
            Assert.Equal(expected.Key, actual.Key);
            Assert.Equal(expected.Value, actual.Value);
        }

        public static Entity CreateSimpleFact(int id, string value) =>
            new Entity(new EntityId("/tests/facts"), new Simple(id, value));
    }
}