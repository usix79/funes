using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Indexes;
using Xunit;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    
    public abstract class AbstractCacheTests {
        
        protected abstract ICache CreateCache();
        private readonly ISerializer _ser = new SimpleSerializer<Simple>();

        private async Task Set(ICache cache, params EntityEntry[] entries) {
            foreach (var entry in entries) {
                var setResult = await cache.Set(entry, _ser, default);
                Assert.True(setResult.IsOk, setResult.Error.ToString());
            }
        }

        private async Task<EntityEntry> Get(ICache cache, EntityId eid) {
            var getResult = await cache.Get(eid, _ser, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            return getResult.Value;
        }

        private async Task<Void> UpdateIfNewer(ICache cache, EntityEntry entry) {
            var updateResult = await cache.UpdateIfNewer(entry, _ser, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            return updateResult.Value;
        }

        private async Task<bool> CheckUpdateIfNewer(ICache cache, EntityEntry entry) {
            var updateResult = await cache.UpdateIfNewer(entry, _ser, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            var getResult = await cache.Get(entry.EntId, _ser, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            return getResult.Value.IncId == entry.IncId;
        }

        [Fact]
        public async void GetNonExistingTest() {
            var cache = CreateCache();

            var eid = CreateRandomEntId();
            var getResult = await cache.Get(eid, _ser, default);
            Assert.True(getResult.Error == Error.NotFound);
        }

        [Fact]
        public async void SetTest() {
            var cache = CreateCache();
            
            var stamp = CreateSimpleEntityStamp(new IncrementId("100"));
            await Set(cache, stamp.ToEntry());
            Assert.Equal(stamp.ToEntry(), await Get(cache, stamp.EntId));
        }

        [Fact]
        public async void LatestSetWinTest() {
            var cache = CreateCache();

            var entry1 = CreateSimpleEntityStamp(new IncrementId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("099"));
            var entry3 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("101"));

            await Set(cache, entry1, entry2); 
            Assert.Equal(entry2, await Get(cache, entry1.EntId));

            await Set(cache, entry3); 
            Assert.Equal(entry3, await Get(cache, entry1.EntId));
        }

        [Fact]
        public async void SetNoExist() {
            var cache = CreateCache();

            var entry = EntityEntry.NotExist(CreateRandomEntId());
            await Set(cache, entry);
            Assert.Equal(entry, await Get(cache, entry.EntId));
        }


        [Fact]
        public async void SetNoExistThanOk() {
            var cache = CreateCache();

            var entry1 = EntityEntry.NotExist(CreateRandomEntId());
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("100"));
            await Set(cache, entry1, entry2);
            Assert.Equal(entry2, await Get(cache, entry1.EntId));
        }

        [Fact]
        public async void UpdateSingleEntry() {
            var cache = CreateCache();
            
            var entry1 = CreateSimpleEntityStamp(new IncrementId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("099"));
            var entry3 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("101"));
            
            Assert.True(await CheckUpdateIfNewer(cache, entry1));
            Assert.Equal(entry1, await Get(cache, entry1.EntId));

            Assert.True(await CheckUpdateIfNewer(cache, entry2));
            Assert.Equal(entry2, await Get(cache, entry2.EntId));
            
            Assert.False(await CheckUpdateIfNewer(cache, entry3));
            Assert.Equal(entry2, await Get(cache, entry2.EntId));
        }

        [Fact]
        public async void FirstUpdateMany() {
            var cache = CreateCache();
            
            var entry1 = EntityEntry.NotExist(CreateRandomEntId("c1"));
            var entry2 = EntityEntry.NotExist(CreateRandomEntId("c2"));
            await UpdateIfNewer(cache, entry1);
            await UpdateIfNewer(cache, entry2);
            Assert.Equal(entry1, await Get(cache, entry1.EntId));
            Assert.Equal(entry2, await Get(cache, entry2.EntId));
        }

        [Fact]
        public async void UpdateManyWithFail() {
            var cache = CreateCache();
            
            var entry1 = CreateSimpleEntityStamp(new IncrementId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new IncrementId("101"));

            var otherEntry = EntityEntry.NotExist(CreateRandomEntId("other"));

            Assert.True(await CheckUpdateIfNewer(cache, entry1));
            Assert.Equal(entry1, await Get(cache, entry1.EntId));
            await UpdateIfNewer(cache, entry2);
            await UpdateIfNewer(cache, otherEntry);
            Assert.Equal(entry1, await Get(cache, entry1.EntId));
            Assert.Equal(otherEntry, await Get(cache, otherEntry.EntId));
        }

        private Event CreateEvent(string incIdStr, IndexOp.Kind kind, string key, string tag) {
            var incId = new IncrementId(incIdStr);
            var rec = new IndexRecord {new(kind, key, tag)};
            var arr = new byte[IndexHelpers.CalcSize(rec)];
            IndexHelpers.Serialize(rec, arr);
            return new Event(incId, arr);
        }
        
        [Fact]
        public async void UpdateEvents() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", IndexOp.Kind.AddTag, "key1", "tag1"),
                CreateEvent("101", IndexOp.Kind.ClearTags, "key1", ""),
                CreateEvent("102-Ы", IndexOp.Kind.AddTag, "key2", "Фраг")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events[0].IncId, getResult.Value.First); 
            Assert.Equal(events[^1].IncId, getResult.Value.Last);

            var reader = new IndexRecordReader(getResult.Value.Data);
            foreach (var evt in events) {
                Assert.True(reader.MoveNext());
                var singleReader = new IndexRecordReader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void UpdateEventsIfExists() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events1 = new[] {
                CreateEvent("100", IndexOp.Kind.AddTag, "key1", "tag1"),
                CreateEvent("101", IndexOp.Kind.ClearTags, "key1", ""),
                CreateEvent("102", IndexOp.Kind.AddTag, "key2", "tag2")
            };
            var events2 = new[] {
                CreateEvent("103", IndexOp.Kind.AddTag, "key3", "tag3"),
                CreateEvent("104", IndexOp.Kind.ClearTags, "key3", ""),
                CreateEvent("105", IndexOp.Kind.AddTag, "key1", "tag23")
            };
            var updateResult1 = await cache.UpdateEventsIfNotExists(entId, events1, default);
            Assert.True(updateResult1.IsOk, updateResult1.Error.ToString());

            var updateResult2 = await cache.UpdateEventsIfNotExists(entId, events2, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events1[0].IncId, getResult.Value.First); 
            Assert.Equal(events1[^1].IncId, getResult.Value.Last);

            var reader = new IndexRecordReader(getResult.Value.Data);
            foreach (var evt in events1) {
                Assert.True(reader.MoveNext());
                var singleReader = new IndexRecordReader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void AppendEventToEmptyCache() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var evt = CreateEvent("100", IndexOp.Kind.AddTag, "key1", "tag1");
            var appendResult = await cache.AppendEvent(entId, evt, default);
            Assert.Equal(Error.NotFound, appendResult.Error);
        }

        [Fact]
        public async void AppendEvent() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", IndexOp.Kind.AddTag, "key1", "tag1"),
                CreateEvent("101", IndexOp.Kind.ClearTags, "key1", ""),
                CreateEvent("102", IndexOp.Kind.AddTag, "key2", "tag2")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            
            var evt1 = CreateEvent("104", IndexOp.Kind.AddTag, "key3", "tag42");
            var appendResult1 = await cache.AppendEvent(entId, evt1, default);
            Assert.True(appendResult1.IsOk, appendResult1.Error.ToString());

            var evt2 = CreateEvent("103", IndexOp.Kind.ReplaceTags, "key1", "tag007");
            var appendResult2 = await cache.AppendEvent(entId, evt2, default);
            Assert.True(appendResult2.IsOk, appendResult2.Error.ToString());

            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            Assert.Equal(events[0].IncId, getResult.Value.First); 
            Assert.Equal(evt2.IncId, getResult.Value.Last);

            var reader = new IndexRecordReader(getResult.Value.Data);
            foreach (var evt in events.Append(evt1).Append(evt2)) {
                Assert.True(reader.MoveNext());
                var singleReader = new IndexRecordReader(evt.Data);
                Assert.True(singleReader.MoveNext());
                Assert.Equal(reader.Current, singleReader.Current);
            }
        }

        [Fact]
        public async void TruncateEvents() {
            var cache = CreateCache();
            var entId = CreateRandomEntId("indexes");
            var events = new[] {
                CreateEvent("100", IndexOp.Kind.AddTag, "key1", "tag1"),
                CreateEvent("101", IndexOp.Kind.ClearTags, "key1", ""),
                CreateEvent("102", IndexOp.Kind.AddTag, "key2", "tag2")
            };
            var updateResult = await cache.UpdateEventsIfNotExists(entId, events, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var truncResult = await cache.TruncateEvents(entId, events[^2].IncId, default);
            Assert.True(truncResult.IsOk, truncResult.Error.ToString());
            
            var getResult = await cache.GetEventLog(entId, default);
            Assert.True(getResult.IsOk, getResult.Error.ToString());
            
            Assert.Equal(events[^1].IncId, getResult.Value.First); 
            Assert.Equal(events[^1].IncId, getResult.Value.Last);

            var reader = new IndexRecordReader(getResult.Value.Data);
            Assert.True(reader.MoveNext());
            var singleReader = new IndexRecordReader(events[^1].Data);
            Assert.True(singleReader.MoveNext());
            Assert.Equal(reader.Current, singleReader.Current);
        }
    }
}