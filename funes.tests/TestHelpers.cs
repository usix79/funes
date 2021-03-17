using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Funes.Tests {
    public static class TestHelpers {
        
        private static readonly Random Rand = new Random(DateTime.Now.Millisecond);

        public static string RandomString(int length) {
            var txt = new StringBuilder(length);
            for (var i = 0; i < length; i++) {
                txt.Append((char) ('a' + Rand.Next(25)));
            }
            return txt.ToString();
        }

        private static Stream StringToStream(string str) 
            => new MemoryStream(Encoding.UTF8.GetBytes(str));

        private static string StreamToString(Stream content) {
            using MemoryStream stream = new MemoryStream();
            content.CopyTo(stream);
            return Encoding.UTF8.GetString(stream.ToArray());
        } 

        public static Mem CreateRandomMem(MemKey? key = null) {
            
            var nonNullKey = key ?? new MemKey("cat-" + RandomString(10), "id-" + RandomString(10));
            
            var headers = new NameValueCollection {
                {"key-" + RandomString(10), "value-" + RandomString(10)},
                {"key-" + RandomString(10), "value-" + RandomString(10)},
                {"key-" + RandomString(10), "value-" + RandomString(10)}
            };

            var content = RandomString(1024);
            
            return new Mem (nonNullKey, headers, StringToStream(content));
        }
        
        public static async Task LoadRandomMemories(IRepository repo) {
            for (var i = 0; i < 2; i++) {
                var cat = "cat-" + RandomString(1);
                for (var j = 0; j < 6; j++) {
                    var id = "id-" + RandomString(5);
                    await repo.PutMem(CreateRandomMem(new MemKey(cat, id)), ReflectionId.NewId());
                }
            }
        }
        
        private static bool CompareNameValueCollections(NameValueCollection nvc1,
            NameValueCollection nvc2)
        {
            return nvc1.AllKeys.OrderBy(key => key)
                       .SequenceEqual(nvc2.AllKeys.OrderBy(key => key))
                   && nvc1.AllKeys.All(key => nvc1[key] == nvc2[key]);
        }        

        public static void AssertMemEquals(Mem expected, Mem? actual) {
            Assert.NotNull(actual);
            if (actual != null) {
                Assert.Equal(expected.Key, actual.Key);
                Assert.True(CompareNameValueCollections(expected.Headers, actual.Headers));
            }
        }

        public static void AssertMemChanged(Mem expected, Mem? actual) {
            Assert.NotNull(actual);
            if (actual != null) {
                Assert.Equal(expected.Key, actual.Key);
                Assert.False(CompareNameValueCollections(expected.Headers, actual.Headers));
            }
        }
        
       
    }
}