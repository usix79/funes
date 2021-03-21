using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Funes.Tests {
    public abstract class AbstractRepoTests {
        protected abstract IRepository CreateRepo();
        
        [Fact]
        public async void GetNonExistingTest() {
            var repo = CreateRepo();

            var testMemId = new MemId("TestCategory", "TestId");
            var testMemKey = new MemKey(testMemId, new ReflectionId("TestReflectionId"));
            var mem = await repo.Get(testMemKey, Serde.Decoder<Simple>);
            Assert.True(mem.Error == Error.NotFound);
        }

        [Fact]
        public async void PutTest() {
            var repo = CreateRepo();

            var testReflectionId = ReflectionId.NewId();
            var testMem = TestHelpers.CreateSimpleMem(testReflectionId);
            
            var putResult = await repo.Put(testMem, Serde.Encoder);
            Assert.True(putResult.IsOk);
            
            var getResult = await repo.Get(testMem.Key, Serde.Decoder<Simple>);
            Assert.True(getResult.IsOk);
            TestHelpers.AssertMemEquals(testMem, getResult.Value);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = CreateRepo();
            
            var id = new MemId("cat-s", "id-b2");
            
            var testReflectionId1 = ReflectionId.NewId();
            var testMem1 = TestHelpers.CreateSimpleMem(testReflectionId1, id);
            var putResult1 = await repo.Put(testMem1, Serde.Encoder);
            Assert.True(putResult1.IsOk);

            await Task.Delay(50);
            
            var testReflectionId2 = ReflectionId.NewId();
            var testMem2 = TestHelpers.CreateSimpleMem(testReflectionId2, id);
            var putResult2 = await repo.Put(testMem2, Serde.Encoder);
            Assert.True(putResult2.IsOk);
            
            var historyResult = await repo.GetHistory(id, ReflectionId.Singularity, 1);
            Assert.True(historyResult.IsOk);
            var rids = historyResult.Value.ToArray();
            Assert.Single(rids);
            var rid = rids[0];
            Assert.NotEqual(testReflectionId1, rid);
            Assert.Equal(testReflectionId2, rid);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = CreateRepo();
            
            var key = new MemId("cat-s", "id-b2");
            var history = new List<(Mem<Simple>, ReflectionId)>();
            for (var i = 0; i < 42; i++) {
                var rid = ReflectionId.NewId();
                var mem = TestHelpers.CreateSimpleMem(rid, key);
                history.Add((mem, rid));
                var putResult = await repo.Put(mem, Serde.Encoder);
                Assert.True(putResult.IsOk);
                await Task.Delay(10);
            }
            
            var historyResult = await repo.GetHistory(key, history[7].Item2, 3);
            Assert.True(historyResult.IsOk);
            var rids = historyResult.Value.ToArray();
            Assert.Equal(3, rids.Length);
            Assert.Equal(history[6].Item2, rids[0]);
            Assert.Equal(history[5].Item2, rids[1]);
            Assert.Equal(history[4].Item2, rids[2]);
            
            historyResult = await repo.GetHistory(key, history[0].Item2, 3);
            Assert.True(historyResult.IsOk);
            rids = historyResult.Value.ToArray();
            Assert.Empty(rids);

            historyResult = await repo.GetHistory(key, ReflectionId.Singularity, 2);
            Assert.True(historyResult.IsOk);
            rids = historyResult.Value.ToArray();
            Assert.Equal(2, rids.Length);
            Assert.Equal(history[41].Item2, rids[0]);
            Assert.Equal(history[40].Item2, rids[1]);

            historyResult = await repo.GetHistory(key, history[2].Item2, 5);
            Assert.True(historyResult.IsOk);
            rids = historyResult.Value.ToArray();
            Assert.Equal(2, rids.Length);
            Assert.Equal(history[1].Item2, rids[0]);
            Assert.Equal(history[0].Item2, rids[1]);
        }
    }
}