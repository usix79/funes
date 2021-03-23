using System;
using System.Threading.Tasks;
using Xunit;

namespace Funes.Tests {
    public static class TestHelpers {
        
        private static readonly Random Rand = new Random(DateTime.Now.Millisecond);

        private static string RandomString(int length) =>
            string.Create(length, length, (span, n) => {
                for (var i = 0; i < n; i++) {
                    span[i] = (char) ('a' + Rand.Next(25));
                }
            });
        
        public static MemStamp CreateSimpleMem(ReflectionId rid, MemId? key = null) {
            
            var nonNullKey = key ?? new MemId("cat-" + RandomString(10), "id-" + RandomString(10));
            
            var content = new Simple(Rand.Next(1024), RandomString(1024));
            
            return new MemStamp (new Mem(nonNullKey, content), rid);
        }
        
        public static async Task LoadRandomMemories(Mem.IRepository repo) {
            for (var i = 0; i < 2; i++) {
                var cat = "cat-" + RandomString(1);
                for (var j = 0; j < 6; j++) {
                    var id = "id-" + RandomString(5);
                    await repo.Put(CreateSimpleMem(ReflectionId.NewId(),new MemId(cat, id)), Serde.Encoder);
                }
            }
        }
        
        public static void AssertMemEquals(MemStamp expected, MemStamp actual) {
            Assert.Equal(expected.Key, actual.Key);
            Assert.Equal(expected.Value, actual.Value);
        }
    }
}