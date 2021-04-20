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

            var testMemId = new EntityId("TestCategory", "TestId");
            var testMemKey = new StampKey(testMemId, new IncrementId("TestReflectionId"));
            var mem = await repo.Load(testMemKey, default);
            Assert.True(mem.Error == Error.NotFound);
        }

        [Fact]
        public async void SaveTest() {
            var repo = CreateRepo();

            var testIncId = IncrementId.NewId();
            var testStamp = TestHelpers.CreateSimpleStamp(testIncId);
            
            var putResult = await repo.Save(testStamp, default);
            Assert.True(putResult.IsOk);
            
            var getResult = await repo.Load(testStamp.Key, default);
            Assert.True(getResult.IsOk);
            Assert.Equal(testStamp, getResult.Value);
        }

        [Fact]
        public async void GetLastTest() {
            var repo = CreateRepo();
            
            var id = new EntityId("cat-s", "id-b2");
            
            var testIncId1 = IncrementId.NewId();
            var testStamp1 = TestHelpers.CreateSimpleStamp(testIncId1, id);
            var putResult1 = await repo.Save(testStamp1, default);
            Assert.True(putResult1.IsOk);

            await Task.Delay(50);
            
            var testReflectionId2 = IncrementId.NewId();
            var testStamp2 = TestHelpers.CreateSimpleStamp(testReflectionId2, id);
            var putResult2 = await repo.Save(testStamp2, default);
            Assert.True(putResult2.IsOk);
            
            var historyResult = await repo.HistoryBefore(id, IncrementId.Singularity, 1);
            Assert.True(historyResult.IsOk);
            var incIds = historyResult.Value.ToArray();
            Assert.Single(incIds);
            var incId = incIds[0];
            Assert.NotEqual(testIncId1, incId);
            Assert.Equal(testReflectionId2, incId);
        }
        
        [Fact]
        public async void SaveEventTest() {
            var repo = CreateRepo();

            var testEid = TestHelpers.CreateRandomEntId();
            var testIncId = IncrementId.NewId();
            var testEvent = TestHelpers.CreateEvent(testIncId);
            var stamp = new BinaryStamp(testEid.CreateStampKey(testIncId), new BinaryData("evt", testEvent.Data));
            
            var saveResult = await repo.Save(stamp, default);
            Assert.True(saveResult.IsOk, saveResult.Error.ToString());
            
            var loadResult = await repo.Load(testEid.CreateStampKey(testIncId), default);
            Assert.True(loadResult.IsOk, loadResult.Error.ToString());
            TestHelpers.AssertEventsEqual(testEvent, new Event(testIncId, loadResult.Value.Data.Memory));
        }

        [Fact]
        public async void GetHistoryBeforeTest() {
            var repo = CreateRepo();
            
            var key = new EntityId("cat-s", "id-b2");
            var history = new List<(BinaryStamp, IncrementId)>();
            for (var i = 0; i < 12; i++) {
                var incId = new IncrementId((100-i).ToString("d4"));
                var stamp = TestHelpers.CreateSimpleStamp(incId, key);
                history.Add((stamp, incId));
                var putResult = await repo.Save(stamp, default);
                Assert.True(putResult.IsOk);
            }
            
            var historyResult = await repo.HistoryBefore(key, history[7].Item2, 3);
            Assert.True(historyResult.IsOk);
            var incIds = historyResult.Value.ToArray();
            Assert.Equal(3, incIds.Length);
            Assert.Equal(history[6].Item2, incIds[0]);
            Assert.Equal(history[5].Item2, incIds[1]);
            Assert.Equal(history[4].Item2, incIds[2]);
            
            historyResult = await repo.HistoryBefore(key, history[0].Item2, 3);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Empty(incIds);

            historyResult = await repo.HistoryBefore(key, IncrementId.Singularity, 2);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Equal(2, incIds.Length);
            Assert.Equal(history[11].Item2, incIds[0]);
            Assert.Equal(history[10].Item2, incIds[1]);

            historyResult = await repo.HistoryBefore(key, history[2].Item2, 5);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Equal(2, incIds.Length);
            Assert.Equal(history[1].Item2, incIds[0]);
            Assert.Equal(history[0].Item2, incIds[1]);
        }
        
        [Fact]
        public async void GetHistoryAfterTest() {
            var repo = CreateRepo();
            
            var key = new EntityId("cat-s", "id-b2");
            var history = new List<(BinaryStamp, IncrementId)>();
            for (var i = 0; i < 12; i++) {
                var incId = new IncrementId((100-i).ToString("d4"));
                var stamp = TestHelpers.CreateSimpleStamp(incId, key);
                history.Add((stamp, incId));
                var putResult = await repo.Save(stamp, default);
                Assert.True(putResult.IsOk);
            }
            
            var historyResult = await repo.HistoryAfter(key, history[8].Item2);
            Assert.True(historyResult.IsOk);
            var incIds = historyResult.Value.ToArray();
            Assert.Equal(3, incIds.Length);
            Assert.Equal(history[9].Item2, incIds[0]);
            Assert.Equal(history[10].Item2, incIds[1]);
            Assert.Equal(history[11].Item2, incIds[2]);
            
            historyResult = await repo.HistoryAfter(key, history[11].Item2);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Empty(incIds);

            historyResult = await repo.HistoryAfter(key, IncrementId.BigBang);
            Assert.True(historyResult.IsOk);
            incIds = historyResult.Value.ToArray();
            Assert.Equal(history.Count, incIds.Length);
            for (var i = 0; i < history.Count; i++) {
                Assert.Equal(history[i].Item2, incIds[i]);
            }
        }
        
    }
}