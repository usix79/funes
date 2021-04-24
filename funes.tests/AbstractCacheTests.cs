using System.Linq;
using System.Threading.Tasks;
using Funes.Sets;
using Xunit;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    
    public abstract class AbstractCacheTests {
        
        protected abstract ICache CreateCache();

        private async Task Set(ICache cache, params BinaryStamp[] stamps) {
            foreach (var stamp in stamps) {
                var setResult = await cache.Set(stamp, default);
                Assert.True(setResult.IsOk, setResult.Error.ToString());
            }
        }

        private async Task<BinaryStamp> Get(ICache cache, EntityId eid) {
            var getResult = await cache.Get(eid, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            return getResult.Value;
        }

        private async Task UpdateIfNewer(ICache cache, BinaryStamp stamp) {
            var updateResult = await cache.UpdateIfNewer(stamp, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
        }

        private async Task<bool> CheckUpdateIfNewer(ICache cache, BinaryStamp stamp) {
            var updateResult = await cache.UpdateIfNewer(stamp, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            var getResult = await cache.Get(stamp.Eid, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            return getResult.Value.IncId == stamp.IncId;
        }

        [Fact]
        public async void GetNonExistingTest() {
            var cache = CreateCache();

            var eid = CreateRandomEntId();
            var getResult = await cache.Get(eid, default);
            Assert.True(getResult.Error == Error.NotFound);
        }

        [Fact]
        public async void SetTest() {
            var cache = CreateCache();
            
            var stamp = CreateSimpleStamp(new IncrementId("100"));
            await Set(cache, stamp);
            Assert.Equal(stamp, await Get(cache, stamp.Eid));
        }

        [Fact]
        public async void LatestSetWinTest() {
            var cache = CreateCache();

            var stamp1 = CreateSimpleStamp(new IncrementId("100"));
            var stamp2 = CreateSimpleStamp(new IncrementId("099"), stamp1.Eid); 
            var stamp3 = CreateSimpleStamp(new IncrementId("101"), stamp1.Eid);

            await Set(cache, stamp1, stamp2); 
            Assert.Equal(stamp2, await Get(cache, stamp1.Eid));

            await Set(cache, stamp3); 
            Assert.Equal(stamp3, await Get(cache, stamp1.Eid));
        }

        [Fact]
        public async void SetNoExist() {
            var cache = CreateCache();

            var stamp = BinaryStamp.Empty(CreateRandomEntId()); 
            await Set(cache, stamp);
            Assert.Equal(stamp, await Get(cache, stamp.Eid));
        }


        [Fact]
        public async void SetNoExistThanOk() {
            var cache = CreateCache();

            var stamp1 = BinaryStamp.Empty(CreateRandomEntId());
            var stamp2 = CreateSimpleStamp(new IncrementId("100"), stamp1.Eid); 
            await Set(cache, stamp1, stamp2);
            Assert.Equal(stamp2, await Get(cache, stamp1.Eid));
        }

        [Fact]
        public async void UpdateSingleEntry() {
            var cache = CreateCache();
            
            var stamp1 = CreateSimpleStamp(new IncrementId("100"));
            var stamp2 = CreateSimpleStamp(new IncrementId("099"), stamp1.Eid); 
            var stamp3 = CreateSimpleStamp(new IncrementId("101"), stamp1.Eid); 
            
            Assert.True(await CheckUpdateIfNewer(cache, stamp1));
            Assert.Equal(stamp1, await Get(cache, stamp1.Eid));

            Assert.True(await CheckUpdateIfNewer(cache, stamp2));
            Assert.Equal(stamp2, await Get(cache, stamp2.Eid));
            
            Assert.False(await CheckUpdateIfNewer(cache, stamp3));
            Assert.Equal(stamp2, await Get(cache, stamp2.Eid));
        }

        [Fact]
        public async void FirstUpdateMany() {
            var cache = CreateCache();
            
            var stamp1 = BinaryStamp.Empty(CreateRandomEntId("c1"));
            var stamp2 = BinaryStamp.Empty(CreateRandomEntId("c2"));
            await UpdateIfNewer(cache, stamp1);
            await UpdateIfNewer(cache, stamp2);
            Assert.Equal(stamp1, await Get(cache, stamp1.Eid));
            Assert.Equal(stamp2, await Get(cache, stamp2.Eid));
        }

        [Fact]
        public async void UpdateManyWithFail() {
            var cache = CreateCache();
            
            var stamp1 = CreateSimpleStamp(new IncrementId("100"));
            var stamp2 = CreateSimpleStamp(new IncrementId("101"), stamp1.Eid);
            var otherStamp = BinaryStamp.Empty(CreateRandomEntId("other")); 

            Assert.True(await CheckUpdateIfNewer(cache, stamp1));
            Assert.Equal(stamp1, await Get(cache, stamp1.Eid));
            await UpdateIfNewer(cache, stamp2);
            await UpdateIfNewer(cache, otherStamp);
            Assert.Equal(stamp1, await Get(cache, stamp1.Eid));
            Assert.Equal(otherStamp, await Get(cache, otherStamp.Eid));
        }

        private Event CreateEvent(string incIdStr, SetOp.Kind kind, string tag) {
            var incId = new IncrementId(incIdStr);
            var rec = new SetRecord {new(kind, tag)};
            var data = SetRecord.Builder.EncodeRecord(rec);
            return new Event(incId, data.Memory);
        }
        
        [Fact]
        public async void UpdateEvents() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", SetOp.Kind.Add, "tag1"),
                CreateEvent("101", SetOp.Kind.Clear, ""),
                CreateEvent("102-Ы", SetOp.Kind.Add, "Фраг")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events[0].IncId, getResult.Value.First); 
            Assert.Equal(events[^1].IncId, getResult.Value.Last);

            var reader = new SetRecord.Reader(getResult.Value.Memory);
            foreach (var evt in events) {
                Assert.True(reader.MoveNext());
                var singleReader = new SetRecord.Reader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void UpdateEventsIfExists() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events1 = new[] {
                CreateEvent("100", SetOp.Kind.Add, "tag1"),
                CreateEvent("101", SetOp.Kind.Clear, ""),
                CreateEvent("102", SetOp.Kind.Add, "tag2")
            };
            var events2 = new[] {
                CreateEvent("103", SetOp.Kind.Add, "tag3"),
                CreateEvent("104", SetOp.Kind.Clear, ""),
                CreateEvent("105", SetOp.Kind.Add, "tag23")
            };
            var updateResult1 = await cache.UpdateEventsIfNotExists(entId, events1, default);
            Assert.True(updateResult1.IsOk, updateResult1.Error.ToString());

            var updateResult2 = await cache.UpdateEventsIfNotExists(entId, events2, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events1[0].IncId, getResult.Value.First); 
            Assert.Equal(events1[^1].IncId, getResult.Value.Last);

            var reader = new SetRecord.Reader(getResult.Value.Memory);
            foreach (var evt in events1) {
                Assert.True(reader.MoveNext());
                var singleReader = new SetRecord.Reader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void AppendEventToEmptyCache() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var evt = CreateEvent("100", SetOp.Kind.Add, "tag1");
            var appendResult = await cache.AppendEvent(entId, evt, default);
            Assert.Equal(Error.NotFound, appendResult.Error);
        }

        [Fact]
        public async void AppendEvent() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", SetOp.Kind.Add, "tag1"),
                CreateEvent("101", SetOp.Kind.Clear, ""),
                CreateEvent("102", SetOp.Kind.Add, "tag2")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            
            var evt1 = CreateEvent("104", SetOp.Kind.Add, "tag42");
            var appendResult1 = await cache.AppendEvent(entId, evt1, default);
            Assert.True(appendResult1.IsOk, appendResult1.Error.ToString());

            var evt2 = CreateEvent("103", SetOp.Kind.ReplaceWith, "tag007");
            var appendResult2 = await cache.AppendEvent(entId, evt2, default);
            Assert.True(appendResult2.IsOk, appendResult2.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            Assert.Equal(events[0].IncId, getResult.Value.First); 
            Assert.Equal(evt2.IncId, getResult.Value.Last);

            var reader = new SetRecord.Reader(getResult.Value.Memory);
            foreach (var evt in events.Append(evt1).Append(evt2)) {
                Assert.True(reader.MoveNext());
                var singleReader = new SetRecord.Reader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void TruncateEvents() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", SetOp.Kind.Add, "tag1"),
                CreateEvent("101", SetOp.Kind.Clear, ""),
                CreateEvent("102", SetOp.Kind.Add, "tag2")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var truncResult = await cache.TruncateEvents(entId, events[^2].IncId, default);
            Assert.True(truncResult.IsOk, truncResult.Error.ToString());
            
            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events[^1].IncId, getResult.Value.First); 
            Assert.Equal(events[^1].IncId, getResult.Value.Last);

            var reader = new SetRecord.Reader(getResult.Value.Memory);
            Assert.True(reader.MoveNext());
            var singleReader = new SetRecord.Reader(events[^1].Data);
            Assert.True(singleReader.MoveNext());
            Assert.Equal(reader.Current, singleReader.Current);
        }
    }
}