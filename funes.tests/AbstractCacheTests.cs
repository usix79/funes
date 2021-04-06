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
    }
}