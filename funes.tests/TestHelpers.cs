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
        
        public static EntityId CreateRandomEid(string? cat = null) =>
            new (cat ?? "cat-" + RandomString(10), "id-" + RandomString(10));

        public static Simple CreateRandomValue() =>
            new (Rand.Next(1024), RandomString(1024));    

        public static EntityStamp CreateSimpleEntityStamp(CognitionId cid, EntityId? eid = null) =>
            new EntityStamp (new Entity(eid??CreateRandomEid(), CreateRandomValue()), cid);
        
        public static void AssertEntitiesEqual(EntityStamp expected, EntityStamp actual) {
            Assert.Equal(expected.Key, actual.Key);
            Assert.Equal(expected.Value, actual.Value);
        }
    }
}