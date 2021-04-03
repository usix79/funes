using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;

namespace Funes.Tests {
    public abstract class AbstractRepoTests {
        protected abstract IRepository CreateRepo();
        private readonly ISerializer _simpleSerializer = new SimpleSerializer<Simple>();

        [Fact]
        public async void GetNonExistingTest() {
            var repo = CreateRepo();

            var testMemId = new EntityId("TestCategory", "TestId");
            var testMemKey = new EntityStampKey(testMemId, new IncrementId("TestReflectionId"));
            var mem = await repo.Load(testMemKey, _simpleSerializer, default);
            Assert.True(mem.Error == Error.NotFound);
        }

        [Fact]
        public async void PutTest() {
            var repo = CreateRepo();

            var testReflectionId = IncrementId.NewId();
            var testMem = TestHelpers.CreateSimpleEntityStamp(testReflectionId);
            
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
            
            var testReflectionId1 = IncrementId.NewId();
            var testMem1 = TestHelpers.CreateSimpleEntityStamp(testReflectionId1, id);
            var putResult1 = await repo.Save(testMem1, _simpleSerializer, default);
            Assert.True(putResult1.IsOk);

            await Task.Delay(50);
            
            var testReflectionId2 = IncrementId.NewId();
            var testMem2 = TestHelpers.CreateSimpleEntityStamp(testReflectionId2, id);
            var putResult2 = await repo.Save(testMem2, _simpleSerializer, default);
            Assert.True(putResult2.IsOk);
            
            var historyResult = await repo.History(id, IncrementId.Singularity, 1);
            Assert.True(historyResult.IsOk);
            var incIds = historyResult.Value.ToArray();
            Assert.Single(incIds);
            var incId = incIds[0];
            Assert.NotEqual(testReflectionId1, incId);
            Assert.Equal(testReflectionId2, incId);
        }

        [Fact]
        public async void GetHistoryTest() {
            var repo = CreateRepo();
            
            var key = new EntityId("cat-s", "id-b2");
            var history = new List<(EntityStamp, IncrementId)>();
            for (var i = 0; i < 42; i++) {
                var incId = IncrementId.NewId();
                var mem = TestHelpers.CreateSimpleEntityStamp(incId, key);
                history.Add((mem, incId));
                var putResult = await repo.Save(mem, _simpleSerializer, default);
                Assert.True(putResult.IsOk);
                await Task.Delay(10);
            }
            
            var historyResult = await repo.History(key, history[7].Item2, 3);
            Assert.True(historyResult.IsOk);
            var incIds = historyResult.Value.ToArray();
            Assert.Equal(3, incIds.Length);
            Assert.Equal(history[6].Item2, incIds[0]);
            Assert.Equal(history[5].Item2, incIds[1]);
            Assert.Equal(history[4].Item2, incIds[2]);
            
            historyResult = await repo.History(key, history[0].Item2, 3);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Empty(incIds);

            historyResult = await repo.History(key, IncrementId.Singularity, 2);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Equal(2, incIds.Length);
            Assert.Equal(history[41].Item2, incIds[0]);
            Assert.Equal(history[40].Item2, incIds[1]);

            historyResult = await repo.History(key, history[2].Item2, 5);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Equal(2, incIds.Length);
            Assert.Equal(history[1].Item2, incIds[0]);
            Assert.Equal(history[0].Item2, incIds[1]);
        }
    }
}