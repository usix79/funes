using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;

namespace Funes.Tests {
    public abstract class AbstractRepoTests {
        protected abstract IRepository CreateRepo();
        private ISerializer _simpleSerializer = new SimpleSerializer<Simple>();

        [Fact]
        public async void GetNonExistingTest() {
            var repo = CreateRepo();

            var testMemId = new EntityId("TestCategory", "TestId");
            var testMemKey = new EntityStampKey(testMemId, new CognitionId("TestReflectionId"));
            var mem = await repo.Load(testMemKey, _simpleSerializer, default);
            Assert.True(mem.Error == Error.NotFound);
        }

        [Fact]
        public async void PutTest() {
            var repo = CreateRepo();

            var testReflectionId = CognitionId.NewId();
            var testMem = TestHelpers.CreateSimpleEntity(testReflectionId);
            
            var putResult = await repo.Save(testMem, _simpleSerializer, default);
            Assert.True(putResult.IsOk);
            
            var getResult = await repo.Load(testMem.Key, _simpleSerializer, default);
            Assert.True(getResult.IsOk);
            TestHelpers.AssertEntitiesEqual(testMem, getResult.Value);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = CreateRepo();
            
            var id = new EntityId("cat-s", "id-b2");
            
            var testReflectionId1 = CognitionId.NewId();
            var testMem1 = TestHelpers.CreateSimpleEntity(testReflectionId1, id);
            var putResult1 = await repo.Save(testMem1, _simpleSerializer, default);
            Assert.True(putResult1.IsOk);

            await Task.Delay(50);
            
            var testReflectionId2 = CognitionId.NewId();
            var testMem2 = TestHelpers.CreateSimpleEntity(testReflectionId2, id);
            var putResult2 = await repo.Save(testMem2, _simpleSerializer, default);
            Assert.True(putResult2.IsOk);
            
            var historyResult = await repo.History(id, CognitionId.Singularity, 1);
            Assert.True(historyResult.IsOk);
            var cids = historyResult.Value.ToArray();
            Assert.Single(cids);
            var cid = cids[0];
            Assert.NotEqual(testReflectionId1, cid);
            Assert.Equal(testReflectionId2, cid);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = CreateRepo();
            
            var key = new EntityId("cat-s", "id-b2");
            var history = new List<(EntityStamp, CognitionId)>();
            for (var i = 0; i < 42; i++) {
                var cid = CognitionId.NewId();
                var mem = TestHelpers.CreateSimpleEntity(cid, key);
                history.Add((mem, cid));
                var putResult = await repo.Save(mem, _simpleSerializer, default);
                Assert.True(putResult.IsOk);
                await Task.Delay(10);
            }
            
            var historyResult = await repo.History(key, history[7].Item2, 3);
            Assert.True(historyResult.IsOk);
            var cids = historyResult.Value.ToArray();
            Assert.Equal(3, cids.Length);
            Assert.Equal(history[6].Item2, cids[0]);
            Assert.Equal(history[5].Item2, cids[1]);
            Assert.Equal(history[4].Item2, cids[2]);
            
            historyResult = await repo.History(key, history[0].Item2, 3);
            Assert.True(historyResult.IsOk);
            cids = historyResult.Value.ToArray();
            Assert.Empty(cids);

            historyResult = await repo.History(key, CognitionId.Singularity, 2);
            Assert.True(historyResult.IsOk);
            cids = historyResult.Value.ToArray();
            Assert.Equal(2, cids.Length);
            Assert.Equal(history[41].Item2, cids[0]);
            Assert.Equal(history[40].Item2, cids[1]);

            historyResult = await repo.History(key, history[2].Item2, 5);
            Assert.True(historyResult.IsOk);
            cids = historyResult.Value.ToArray();
            Assert.Equal(2, cids.Length);
            Assert.Equal(history[1].Item2, cids[0]);
            Assert.Equal(history[0].Item2, cids[1]);
        }
    }
}