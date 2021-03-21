using System;
using System.Collections.Generic;
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
        
        public static Mem<Simple> CreateSimpleMem(ReflectionId rid, MemId? key = null) {
            
            var nonNullKey = key ?? new MemId("cat-" + RandomString(10), "id-" + RandomString(10));
            
            var headers = new Dictionary<string,string> {
                {"key-" + RandomString(10), "value-" + RandomString(10)},
                {"key-" + RandomString(10), "value-" + RandomString(10)},
                {"key-" + RandomString(10), "value-" + RandomString(10)}
            };

            var content = new Simple(Rand.Next(1024), RandomString(1024));
            
            return new Mem<Simple> (new MemKey(nonNullKey, rid), headers, content);
        }
        
        public static async Task LoadRandomMemories(IRepository repo) {
            for (var i = 0; i < 2; i++) {
                var cat = "cat-" + RandomString(1);
                for (var j = 0; j < 6; j++) {
                    var id = "id-" + RandomString(5);
                    await repo.Put(CreateSimpleMem(ReflectionId.NewId(),new MemId(cat, id)), Serde.Encoder);
                }
            }
        }
        
        public static void AssertMemEquals<T>(Mem<T> expected, Mem<T> actual) {
            Assert.Equal(expected.Key, actual.Key);
            Assert.Equal(expected.Headers, actual.Headers);
            Assert.Equal(expected.Content, actual.Content);
        }
    }
}