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
            
            var testMem = TestHelpers.CreateRandomMem();
            var testReflectionId = ReflectionId.NewId();
            
            await repo.Put(testMem, testReflectionId);
            
            var mem = await repo.Get(testMem.Key, testReflectionId);
            
            TestHelpers.AssertMemEquals(testMem, mem);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = new InMemoryRepository();
            
            await TestHelpers.LoadRandomMemories(repo);

            var key = new MemKey("cats", "idb2");
            
            var testMem1 = TestHelpers.CreateRandomMem(key);
            var testReflectionId1 = ReflectionId.NewId();
            await repo.Put(testMem1, testReflectionId1);

            await Task.Delay(50);
            
            var testMem2 = TestHelpers.CreateRandomMem(key);
            var testReflectionId2 = ReflectionId.NewId();
            await repo.Put(testMem2, testReflectionId2);
            
            var pair = await repo.GetLatest(key);
            
            Assert.NotNull(pair);
            Assert.NotEqual(testReflectionId1, pair?.Item2);
            TestHelpers.AssertMemChanged(testMem1, pair?.Item1);
            Assert.Equal(testReflectionId2, pair?.Item2);
            TestHelpers.AssertMemEquals(testMem2, pair?.Item1);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = new InMemoryRepository();
            
            await TestHelpers.LoadRandomMemories(repo);

            var key = new MemKey("cats", "idb2");
            var history = new List<(Mem, ReflectionId)>();
            for (var i = 0; i < 42; i++) {
                var mem = TestHelpers.CreateRandomMem(key);
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