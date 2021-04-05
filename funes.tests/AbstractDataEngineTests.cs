using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    
    public abstract class AbstractDataEngineTests {
        private readonly ITestOutputHelper _testOutputHelper;

        protected AbstractDataEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        protected abstract IDataEngine CreateEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger);

        private ILogger Logger() => XUnitLogger.CreateLogger(_testOutputHelper);

        private async Task CheckCache(ICache cache, ISerializer ser, EntityEntry entry) {
            var res = await cache.Get(entry.EntId, ser, default);
            Assert.True(res.IsOk, res.Error.ToString());
            Assert.Equal(entry, res.Value);
        }

        private async Task CheckRepo(IRepository repo, ISerializer ser, EntityStamp stamp) {
            var res = await repo.Load(stamp.Key, ser, default);
            Assert.True(res.IsOk, res.Error.ToString());
            Assert.Equal(stamp, res.Value);
        }

        [Fact]
        public async void EmptyTest() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            await de.Flush();
        }

        [Fact]
        public async void RetrieveNotExistedTest() {
            var eid = CreateRandomEntId();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(eid, ser, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.True(retResult.Value.IsNotExist);

            await de.Flush();
        }

        [Fact]
        public async void RetrieveCachedTest() {
            var entry = CreateSimpleEntityStamp(new IncrementId("100")).ToEntry();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            await cache.Set(entry, ser, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(entry.EntId, ser, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(entry, retResult.Value);

            await de.Flush();
        }

        [Fact]
        public async void RetrieveSavedTest() {
            var prevEntry = CreateSimpleEntityStamp(new IncrementId("100")).ToEntry();
            var entry = CreateSimpleEntityStamp(new IncrementId("099"), prevEntry.EntId).ToEntry();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            await repo.Save(prevEntry.ToStamp(), ser, default);
            await repo.Save(entry.ToStamp(), ser, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(entry.EntId, ser, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(entry, retResult.Value);

            await CheckCache(cache, ser, entry);

            await de.Flush();
        }

        [Fact]
        public async void UploadNewTest() {
            var stamp = CreateSimpleEntityStamp(new IncrementId("100"));
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var uploadResult = await de.Upload(new []{stamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.True(uploadResult.Value);

            await CheckCache(cache, ser, stamp.ToEntry());

            await de.Flush();
            
            await CheckRepo(repo, ser, stamp);
        }

        [Fact]
        public async void UploadYoungerTest() {
            var prevStamp = CreateSimpleEntityStamp(new IncrementId("100"));
            var stamp = CreateSimpleEntityStamp(new IncrementId("099"), prevStamp.EntId);
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            await repo.Save(prevStamp, ser, default);

            var uploadResult = await de.Upload(new []{stamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.True(uploadResult.Value);

            await CheckCache(cache, ser, stamp.ToEntry());

            await de.Flush();
            
            await CheckRepo(repo, ser, stamp);
        }

        [Fact]
        public async void TryUploadOlderTest() {
            var nextStamp = CreateSimpleEntityStamp(new IncrementId("098"));
            var stamp = CreateSimpleEntityStamp(new IncrementId("099"), nextStamp.EntId);
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            await cache.Set(nextStamp.ToEntry(), ser, default);

            var uploadResult = await de.Upload(new []{stamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.False(uploadResult.Value);

            await CheckCache(cache, ser, nextStamp.ToEntry());

            await de.Flush();
            
            await CheckRepo(repo, ser, stamp);
        }

        [Fact]
        public async void TryUploadIfRepoHasYoungerAndCacheIsEmpty() {
            var nextStamp = CreateSimpleEntityStamp(new IncrementId("098"));
            var stamp = CreateSimpleEntityStamp(new IncrementId("099"), nextStamp.EntId);
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            await repo.Save(nextStamp, ser, default);

            var uploadResult = await de.Upload(new []{stamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.True(uploadResult.Value);

            await CheckCache(cache, ser, stamp.ToEntry());

            await de.Flush();
            
            await CheckRepo(repo, ser, nextStamp);
        }

        [Fact]
        public async void SkipCache() {
            var stamp = CreateSimpleEntityStamp(new IncrementId("099"));
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());
            
            var uploadResult = await de.Upload(new []{stamp}, ser, default, true);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.False(uploadResult.Value);

            var cacheResult = await cache.Get(stamp.EntId, ser, default);
            Assert.Equal(Error.NotFound, cacheResult.Error);

            await de.Flush();
            
            await CheckRepo(repo, ser, stamp);
        }

        private IEnumerable<EntityStampKey> Keys(params (EntityId, IncrementId)[] keys) => 
            keys.Select(x => new EntityStampKey(x.Item1, x.Item2));

        private async Task AssertCommit(IDataEngine de, bool expectedSuccess,
            IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions) {
        
            var r = await de.TryCommit(premises, conclusions, default);
        
            if (expectedSuccess) {
                Assert.True(r.IsOk, r.Error.ToString());
            }
            else {
                Assert.True(r.Error is Error.CommitError);
            }
        }

        [Fact]
        public async void SuccessCommit() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow;

            var eid = CreateRandomEntId();
            var startIncId = IncrementId.ComposeId(startTime, "testing");
            await AssertCommit(de, true, Keys(), Keys((eid, startIncId)));

            var nextIncId = IncrementId.ComposeId(startTime.AddSeconds(1), "testing");
            await AssertCommit(de, true, Keys((eid, startIncId)), Keys((eid, nextIncId)));
            
            var prevIncId = IncrementId.ComposeId(startTime.AddSeconds(-1), "testing");
            await AssertCommit(de, true, Keys((eid, nextIncId)), Keys((eid, prevIncId)));
        }

        [Fact]
        public async void PiSecWhenCacheEqualsTre() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow;

            var eid = CreateRandomEntId();
            var actualIncId = IncrementId.ComposeId(startTime.AddMinutes(-1), "testing");
            var actualStamp = CreateSimpleEntityStamp(actualIncId, eid);
            await AssertCommit(de, true, Keys(),Keys((eid, actualIncId)));
            var uploadResult = await de.Upload(new[] {actualStamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            var wrongIncId = IncrementId.ComposeId(startTime.AddSeconds(-30), "wrong");

            var currentIncId = IncrementId.ComposeId(startTime, "testing");
            var commitResult = await de.TryCommit(Keys((eid, wrongIncId)), Keys((eid, currentIncId)), default);
            Assert.True(commitResult.Error is Error.CommitError);

            await de.Flush();
            
            // check that actual value is not changed
            var retrieveResult = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(actualStamp.ToEntry(), retrieveResult.Value);
            
            await AssertCommit(de, true, Keys((eid, actualIncId)),Keys((eid, currentIncId)));
        }

        [Fact]
        public async void PiSecWhenCacheNotEqualsTre() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow.AddMinutes(-2);

            var eid = CreateRandomEntId();
            
            var originIncId = IncrementId.ComposeId(startTime, "testing");
            var originStamp = CreateSimpleEntityStamp(originIncId, eid);
            await AssertCommit(de, true, Keys(),Keys((eid, originIncId)));
            var uploadResult = await de.Upload(new[] {originStamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            // 1 minute letter
            // new increment went bad
            var actualIncId = IncrementId.ComposeId(startTime.AddMinutes(1), "testing");
            await AssertCommit(de, true, Keys((eid, originIncId)),Keys((eid, actualIncId)));
            // something went wrong and upload was not performed
            // ...
            // ...
            await de.Flush();
            
            // 1 minute latter...
            // new increment is performed
            var nextIncId = IncrementId.ComposeId(startTime.AddMinutes(2), "testing");

            var retrieveResult = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(originIncId, retrieveResult.Value.IncId);
            // origin incId in cache and actual incId is in transaction engine, so commit should fail
            await AssertCommit(de,false, Keys((eid, retrieveResult.Value.IncId)), Keys((eid, nextIncId)));

            await de.Flush();
            
            // piSec routine should set originValue in cache and transaction engine
            var retrieveResultAfterPiSec = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResultAfterPiSec.IsOk, retrieveResultAfterPiSec.Error.ToString());
            Assert.Equal(originStamp.ToEntry(), retrieveResultAfterPiSec.Value);
            
            // now next incId may be commited
            await AssertCommit(de, true, Keys((eid, retrieveResultAfterPiSec.Value.IncId)),Keys((eid, nextIncId)));
        }

    }
}