using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using Funes.Impl;
using Microsoft.Extensions.Logging;
using Moq;
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
            var res = await cache.Get(entry.Eid, ser, default);
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
            var mockRepo = new Mock<IRepository>();
            var mockCache = new Mock<ICache>();
            var mockTre = new Mock<ITransactionEngine>();

            var de = CreateEngine(mockRepo.Object, mockCache.Object, mockTre.Object, Logger());

            await de.Flush();
        }

        [Fact]
        public async void RetrieveNotExistedTest() {
            var eid = CreateRandomEid();
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
            var entry = CreateSimpleEntityStamp(new CognitionId("100")).ToEntry();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            await cache.Set(entry, ser, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(entry.Eid, ser, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(entry, retResult.Value);

            await de.Flush();
        }

        [Fact]
        public async void RetrieveSavedTest() {
            var prevEntry = CreateSimpleEntityStamp(new CognitionId("100")).ToEntry();
            var entry = CreateSimpleEntityStamp(new CognitionId("099"), prevEntry.Eid).ToEntry();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            await repo.Save(prevEntry.ToStamp(), ser, default);
            await repo.Save(entry.ToStamp(), ser, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(entry.Eid, ser, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(entry, retResult.Value);

            await CheckCache(cache, ser, entry);

            await de.Flush();
        }

        [Fact]
        public async void UploadNewTest() {
            var stamp = CreateSimpleEntityStamp(new CognitionId("100"));
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
            var prevStamp = CreateSimpleEntityStamp(new CognitionId("100"));
            var stamp = CreateSimpleEntityStamp(new CognitionId("099"), prevStamp.Eid);
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
            var nextStamp = CreateSimpleEntityStamp(new CognitionId("098"));
            var stamp = CreateSimpleEntityStamp(new CognitionId("099"), nextStamp.Eid);
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
            var nextStamp = CreateSimpleEntityStamp(new CognitionId("098"));
            var stamp = CreateSimpleEntityStamp(new CognitionId("099"), nextStamp.Eid);
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
            var stamp = CreateSimpleEntityStamp(new CognitionId("099"));
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());
            
            var uploadResult = await de.Upload(new []{stamp}, ser, default, true);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            Assert.False(uploadResult.Value);

            var cacheResult = await cache.Get(stamp.Eid, ser, default);
            Assert.Equal(Error.NotFound, cacheResult.Error);

            await de.Flush();
            
            await CheckRepo(repo, ser, stamp);
        }

        private IEnumerable<EntityStampKey> Keys(params (EntityId, CognitionId)[] keys) => 
            keys.Select(x => new EntityStampKey(x.Item1, x.Item2));

        private async Task AssertCommit(IDataEngine de, bool expectedSuccess,
            IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions) {
        
            var r = await de.Commit(premises, conclusions, default);
        
            if (expectedSuccess) {
                Assert.True(r.IsOk, r.Error.ToString());
                Assert.True(r.Value);
            }
            else {
                Assert.True(r.Error is Error.TransactionError);
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

            var eid = CreateRandomEid();
            var startCid = CognitionId.ComposeId(startTime, "testing");
            await AssertCommit(de, true, Keys(), Keys((eid, startCid)));

            var nextCid = CognitionId.ComposeId(startTime.AddSeconds(1), "testing");
            await AssertCommit(de, true, Keys((eid, startCid)), Keys((eid, nextCid)));
            
            var prevCid = CognitionId.ComposeId(startTime.AddSeconds(-1), "testing");
            await AssertCommit(de, true, Keys((eid, nextCid)), Keys((eid, prevCid)));
        }

        [Fact]
        public async void PiSecWhenCacheEqualsTre() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow;

            var eid = CreateRandomEid();
            var actualCid = CognitionId.ComposeId(startTime.AddMinutes(-1), "testing");
            var actualStamp = CreateSimpleEntityStamp(actualCid, eid);
            await AssertCommit(de, true, Keys(),Keys((eid, actualCid)));
            var uploadResult = await de.Upload(new[] {actualStamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            var wrongCid = CognitionId.ComposeId(startTime.AddSeconds(-30), "wrong");

            var currentCid = CognitionId.ComposeId(startTime, "testing");
            var commitResult = await de.Commit(Keys((eid, wrongCid)), Keys((eid, currentCid)), default);
            Assert.True(commitResult.Error is Error.TransactionError);

            await de.Flush();
            
            // check that actual value is not changed
            var retrieveResult = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(actualStamp.ToEntry(), retrieveResult.Value);
            
            await AssertCommit(de, true, Keys((eid, actualCid)),Keys((eid, currentCid)));
        }

        [Fact]
        public async void PiSecWhenCacheNotEqualsTre() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow.AddMinutes(-2);

            var eid = CreateRandomEid();
            
            var originCid = CognitionId.ComposeId(startTime, "testing");
            var originStamp = CreateSimpleEntityStamp(originCid, eid);
            await AssertCommit(de, true, Keys(),Keys((eid, originCid)));
            var uploadResult = await de.Upload(new[] {originStamp}, ser, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            // 1 minute letter
            // new cognition went bad
            var actualCid = CognitionId.ComposeId(startTime.AddMinutes(1), "testing");
            await AssertCommit(de, true, Keys((eid, originCid)),Keys((eid, actualCid)));
            // something went wrong and upload was not performed
            // ...
            // ...
            await de.Flush();
            
            // 1 minute latter...
            // new cognition is performed
            var nextCid = CognitionId.ComposeId(startTime.AddMinutes(2), "testing");

            var retrieveResult = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(originCid, retrieveResult.Value.Cid);
            // origin cid in cache and actual cid is in transaction engine, so commit should fail
            await AssertCommit(de,false, Keys((eid, retrieveResult.Value.Cid)), Keys((eid, nextCid)));

            await de.Flush();
            
            // piSec routine should set originValue in cache and transaction engine
            var retrieveResultAfterPiSec = await de.Retrieve(eid, ser, default);
            Assert.True(retrieveResultAfterPiSec.IsOk, retrieveResultAfterPiSec.Error.ToString());
            Assert.Equal(originStamp.ToEntry(), retrieveResultAfterPiSec.Value);
            
            // now next cid may be commited
            await AssertCommit(de, true, Keys((eid, retrieveResultAfterPiSec.Value.Cid)),Keys((eid, nextCid)));
        }

    }
}