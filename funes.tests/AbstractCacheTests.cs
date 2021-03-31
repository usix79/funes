using System.Threading.Tasks;
using Funes.Impl;
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

        private async Task<bool> UpdateIfOlder(ICache cache, params EntityEntry[] entries) {
            var updateResult = await cache.UpdateIfOlder(entries, _ser, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            return updateResult.Value;
        }

        [Fact]
        public async void GetNonExistingTest() {
            var cache = CreateCache();

            var eid = CreateRandomEid();
            var getResult = await cache.Get(eid, _ser, default);
            Assert.True(getResult.Error == Error.NotFound);
        }

        [Fact]
        public async void SetTest() {
            var cache = CreateCache();
            
            var stamp = CreateSimpleEntityStamp(new CognitionId("100"));
            await Set(cache, stamp.ToEntry());
            Assert.Equal(stamp.ToEntry(), await Get(cache, stamp.Eid));
        }

        [Fact]
        public async void LatestSetWinTest() {
            var cache = CreateCache();

            var entry1 = CreateSimpleEntityStamp(new CognitionId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("099"));
            var entry3 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("101"));

            await Set(cache, entry1, entry2); 
            Assert.Equal(entry2, await Get(cache, entry1.Eid));

            await Set(cache, entry3); 
            Assert.Equal(entry3, await Get(cache, entry1.Eid));
        }

        [Fact]
        public async void SetNoExist() {
            var cache = CreateCache();

            var entry = EntityEntry.NotExist(CreateRandomEid());
            await Set(cache, entry);
            Assert.Equal(entry, await Get(cache, entry.Eid));
        }


        [Fact]
        public async void SetNoExistThanOk() {
            var cache = CreateCache();

            var entry1 = EntityEntry.NotExist(CreateRandomEid());
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("100"));
            await Set(cache, entry1, entry2);
            Assert.Equal(entry2, await Get(cache, entry1.Eid));
        }

        [Fact]
        public async void UpdateSingleEntry() {
            var cache = CreateCache();
            
            var entry1 = CreateSimpleEntityStamp(new CognitionId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("099"));
            var entry3 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("101"));
            
            Assert.True(await UpdateIfOlder(cache, entry1));
            Assert.Equal(entry1, await Get(cache, entry1.Eid));

            Assert.True(await UpdateIfOlder(cache, entry2));
            Assert.Equal(entry2, await Get(cache, entry2.Eid));
            
            Assert.False(await UpdateIfOlder(cache, entry3));
            Assert.Equal(entry2, await Get(cache, entry2.Eid));
        }

        [Fact]
        public async void FirstUpdateMany() {
            var cache = CreateCache();
            
            var entry1 = EntityEntry.NotExist(CreateRandomEid("c1"));
            var entry2 = EntityEntry.NotExist(CreateRandomEid("c2"));
            Assert.True(await UpdateIfOlder(cache, entry1, entry2));
            Assert.Equal(entry1, await Get(cache, entry1.Eid));
            Assert.Equal(entry2, await Get(cache, entry2.Eid));
        }

        [Fact]
        public async void UpdateManyWithFail() {
            var cache = CreateCache();
            
            var entry1 = CreateSimpleEntityStamp(new CognitionId("100")).ToEntry();
            var entry2 = EntityEntry.Ok(entry1.Entity.MapValue(CreateRandomValue()), new CognitionId("101"));

            var otherEntry = EntityEntry.NotExist(CreateRandomEid("other"));

            Assert.True(await UpdateIfOlder(cache, entry1));
            Assert.Equal(entry1, await Get(cache, entry1.Eid));
            Assert.False(await UpdateIfOlder(cache, entry2, otherEntry));
            Assert.Equal(entry1, await Get(cache, entry1.Eid));
            var getOtherResult = await cache.Get(otherEntry.Eid, _ser, default);
            Assert.True(getOtherResult.Error == Error.NotFound, getOtherResult.Error.ToString());
        }
    }
}