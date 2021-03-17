using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Funes.Tests {
    public abstract class AbstractRepoTests {
        public abstract IRepository CreateRepo();
        
        [Fact]
        public async void GetNonExistingTest() {
            var repo = CreateRepo();

            var testMemKey = new MemKey ("TestCategory", "TestId");
            var testReflectionId = new ReflectionId {Id = "TestReflectionId"};
            var mem = await repo.GetMem(testMemKey, testReflectionId);
            Assert.Null(mem);
        }

        [Fact]
        public async void PutTest() {
            var repo = CreateRepo();

            var testMem = TestHelpers.CreateRandomMem();
            var testReflectionId = ReflectionId.NewId();
            
            await repo.PutMem(testMem, testReflectionId);
            
            var mem = await repo.GetMem(testMem.Key, testReflectionId);
            
            TestHelpers.AssertMemEquals(testMem, mem);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = CreateRepo();
            
            var key = new MemKey("cat-s", "id-b2");
            var testReflectionId1 = ReflectionId.NewId();
            await repo.SetLatestRid(key, testReflectionId1);

            await Task.Delay(50);
            
            var testReflectionId2 = ReflectionId.NewId();
            await repo.SetLatestRid(key, testReflectionId2);
            
            var rid = await repo.GetLatestRid(key);
            
            Assert.NotEqual(testReflectionId1, rid);
            Assert.Equal(testReflectionId2, rid);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = CreateRepo();
            
            var key = new MemKey("cat-s", "id-b2");
            var history = new List<(Mem, ReflectionId)>();
            for (var i = 0; i < 42; i++) {
                var mem = TestHelpers.CreateRandomMem(key);
                var rid = ReflectionId.NewId();
                history.Add((mem, rid));
                await repo.PutMem(mem, rid);
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

            result = await repo.GetHistory(key, ReflectionId.Null, 2);
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