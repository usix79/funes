using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Funes.Tests {
    
    public class InMemoryRepoTests {
        
        private readonly Random _rand = new Random(DateTime.Now.Millisecond);

        private string RandomString(int length) {
            var txt = new StringBuilder(length);
            for (var i = 0; i < length; i++) {
                txt.Append((char) ('a' + _rand.Next(25)));
            }
            return txt.ToString();
        }

        private static Stream StringToStream(string str) 
            => new MemoryStream(Encoding.UTF8.GetBytes(str));

        private static string StreamToString(Stream stream) {
            stream.Position = 0;
            var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        } 

        private Mem CreateRandomMem(MemKey? key = null) {
            
            var nonNullKey = key ?? new MemKey("Cat" + RandomString(10), "id" + RandomString(10));
            
            var headers = new NameValueCollection {
                {"Key" + RandomString(10), "Value" + RandomString(10)},
                {"Key" + RandomString(10), "Value" + RandomString(10)},
                {"Key" + RandomString(10), "Value" + RandomString(10)}
            };
            return new Mem (nonNullKey, headers, StringToStream(RandomString(1024)));
        }

        private async Task LoadRandomMemories(IRepository repo) {
            for (var i = 0; i < 42; i++) {
                var cat = "cat" + RandomString(1);
                for (var j = 0; j < 42; j++) {
                    var id = "id" + RandomString(5);
                    await repo.Put(CreateRandomMem(new MemKey(cat, id)), ReflectionId.NewId());
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

        private void AssertMemEquals(Mem expected, Mem? actual) {
            Assert.NotNull(actual);
            if (actual != null) {
                Assert.Equal(expected.Key, actual.Key);
                Assert.True(CompareNameValueCollections(expected.Headers, actual.Headers));
                Assert.Equal(StreamToString(expected.Data), StreamToString(actual.Data));
            }
        }

        private void AssertMemChanged(Mem expected, Mem? actual) {
            Assert.NotNull(actual);
            if (actual != null) {
                Assert.Equal(expected.Key, actual.Key);
                Assert.False(CompareNameValueCollections(expected.Headers, actual.Headers));
                Assert.NotEqual(StreamToString(expected.Data), StreamToString(actual.Data));
            }
        }
        
        [Fact]
        public async void GetNonExistingTest() {
            var repo = new InMemoryRepository();

            var testMemKey = new MemKey ("TestCategory", "TestId");
            var testReflectionId = new ReflectionId {Id = "TestReflectionId"};
            var mem = await repo.Get(testMemKey, testReflectionId);
            Assert.Null(mem);
        }

        [Fact]
        public async void PutTest() {
            var repo = new InMemoryRepository();
            
            var testMem = CreateRandomMem();
            var testReflectionId = ReflectionId.NewId();
            
            await repo.Put(testMem, testReflectionId);
            
            var mem = await repo.Get(testMem.Key, testReflectionId);
            
            AssertMemEquals(testMem, mem);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = new InMemoryRepository();
            
            await LoadRandomMemories(repo);

            var key = new MemKey("cats", "idb2");
            
            var testMem1 = CreateRandomMem(key);
            var testReflectionId1 = ReflectionId.NewId();
            await repo.Put(testMem1, testReflectionId1);

            await Task.Delay(50);
            
            var testMem2 = CreateRandomMem(key);
            var testReflectionId2 = ReflectionId.NewId();
            await repo.Put(testMem2, testReflectionId2);
            
            var pair = await repo.GetLatest(key);
            
            Assert.NotNull(pair);
            Assert.NotEqual(testReflectionId1, pair?.Item2);
            AssertMemChanged(testMem1, pair?.Item1);
            Assert.Equal(testReflectionId2, pair?.Item2);
            AssertMemEquals(testMem2, pair?.Item1);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = new InMemoryRepository();
            
            await LoadRandomMemories(repo);

            var key = new MemKey("cats", "idb2");
            var history = new List<(Mem, ReflectionId)>();
            for (var i = 0; i < 42; i++) {
                var mem = CreateRandomMem(key);
                var rid = ReflectionId.NewId();
                history.Add((mem, rid));
                await repo.Put(mem, rid);
                await Task.Delay(10);
            }
            
            var result = await repo.GetHistory(key, history[7].Item2, 3);
            var resultList = result.ToList();
            Assert.Equal(3, resultList.Count);
            Assert.Equal(history[6].Item2, resultList[0]);
            Assert.Equal(history[5].Item2, resultList[1]);
            Assert.Equal(history[4].Item2, resultList[2]);
            
            result = await repo.GetHistory(key, history[0].Item2, 3);
            resultList = result.ToList();
            Assert.Empty(resultList);

            result = await repo.GetHistory(key, ReflectionId.Empty, 2);
            resultList = result.ToList();
            Assert.Equal(2, resultList.Count);
            Assert.Equal(history[41].Item2, resultList[0]);
            Assert.Equal(history[40].Item2, resultList[1]);

            result = await repo.GetHistory(key, history[2].Item2, 5);
            resultList = result.ToList();
            Assert.Equal(2, resultList.Count);
            Assert.Equal(history[1].Item2, resultList[0]);
            Assert.Equal(history[0].Item2, resultList[1]);
        }
    }
}