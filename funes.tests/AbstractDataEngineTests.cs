using System;
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

        private async Task CheckCache(ICache cache, BinaryStamp stamp) {
            var res = await cache.Get(stamp.Eid, default);
            Assert.True(res.IsOk, res.Error.ToString());
            Assert.Equal(stamp, res.Value);
        }

        private async Task CheckRepo(IRepository repo, BinaryStamp stamp) {
            var res = await repo.Load(stamp.Key, default);
            Assert.True(res.IsOk, res.Error.ToString());
            Assert.Equal(stamp, res.Value);
        }

        [Fact]
        public async void EmptyTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            await de.Flush();
        }

        [Fact]
        public async void RetrieveNotExistedTest() {
            var eid = CreateRandomEntId();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(eid, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.True(retResult.Value.IsEmpty);

            await de.Flush();
        }

        [Fact]
        public async void RetrieveCachedTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var stamp = CreateSimpleStamp(new IncrementId("100"));
            await cache.Set(stamp, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(stamp.Eid, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(stamp, retResult.Value);

            await de.Flush();
        }

        [Fact]
        public async void RetrieveSavedTest() {
            var prevStamp = CreateSimpleStamp(new IncrementId("100"));
            var stamp = CreateSimpleStamp(new IncrementId("099"), prevStamp.Eid);
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            await repo.Save(prevStamp, default);
            await repo.Save(stamp, default);

            var de = CreateEngine(repo, cache, tre, Logger());

            var retResult = await de.Retrieve(stamp.Eid, default);
            Assert.True(retResult.IsOk, retResult.Error.ToString());
            Assert.Equal(stamp, retResult.Value);

            await CheckCache(cache, stamp);

            await de.Flush();
        }

        [Fact]
        public async void UploadNewTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var stamp = CreateSimpleStamp(new IncrementId("100"));
            var uploadResult = await de.Upload(stamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            await CheckCache(cache, stamp);
            await de.Flush();
            await CheckRepo(repo, stamp);
        }

        [Fact]
        public async void UploadYoungerTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var prevStamp = CreateSimpleStamp(new IncrementId("100"));
            var stamp = CreateSimpleStamp(new IncrementId("099"), prevStamp.Eid);
            await repo.Save(prevStamp, default);

            var uploadResult = await de.Upload(stamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            await CheckCache(cache, stamp);

            await de.Flush();
            
            await CheckRepo(repo, stamp);
        }

        [Fact]
        public async void TryUploadOlderTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var nextStamp = CreateSimpleStamp(new IncrementId("098"));
            var stamp = CreateSimpleStamp(new IncrementId("099"), nextStamp.Eid);
            await cache.Set(nextStamp, default);

            var uploadResult = await de.Upload(stamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            await CheckCache(cache, nextStamp);
            await de.Flush();
            await CheckRepo(repo, stamp);
        }

        [Fact]
        public async void TryUploadIfRepoHasYoungerAndCacheIsEmpty() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var nextStamp = CreateSimpleStamp(new IncrementId("098"));
            var stamp = CreateSimpleStamp(new IncrementId("099"), nextStamp.Eid);
            await repo.Save(nextStamp, default);

            var uploadResult = await de.Upload(stamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            await CheckCache(cache, stamp);

            await de.Flush();
            
            await CheckRepo(repo, nextStamp);
        }

        [Fact]
        public async void SkipCache() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());
            
            var stamp = CreateSimpleStamp(new IncrementId("099"));
            var uploadResult = await de.Upload(stamp, default, true);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            var cacheResult = await cache.Get(stamp.Eid, default);
            Assert.Equal(Error.NotFound, cacheResult.Error);

            await de.Flush();
            
            await CheckRepo(repo, stamp);
        }
        
        private async Task AssertCommit(IDataEngine de, bool expectedSuccess,
            StampKey[] inputs, EntityId[] outputs, IncrementId incId) {
        
            var r = await de.TryCommit(inputs, outputs, incId, default);
        
            if (expectedSuccess) {
                Assert.True(r.IsOk, r.Error.ToString());
            }
            else {
                Assert.True(r.Error is Error.CommitError);
            }
        }

        [Fact]
        public async void SuccessCommit() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow;

            var eid = CreateRandomEntId();
            var startIncId = IncrementId.ComposeId(startTime, "testing");
            await AssertCommit(de, true, EmptyKeys, EntIds(eid), startIncId);

            var nextIncId = IncrementId.ComposeId(startTime.AddSeconds(1), "testing");
            await AssertCommit(de, true, Keys((eid, startIncId)), EntIds(eid), nextIncId);
            
            var prevIncId = IncrementId.ComposeId(startTime.AddSeconds(-1), "testing");
            await AssertCommit(de, true, Keys((eid, nextIncId)), EntIds(eid), prevIncId);
        }

        [Fact]
        public async void PiSecWhenCacheEqualsTre() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow;

            var eid = CreateRandomEntId();
            var actualIncId = IncrementId.ComposeId(startTime.AddMinutes(-1), "testing");
            var actualStamp = CreateSimpleStamp(actualIncId, eid);
            await AssertCommit(de, true, EmptyKeys,EntIds(eid), actualIncId);
            var uploadResult = await de.Upload(actualStamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());

            var wrongIncId = IncrementId.ComposeId(startTime.AddSeconds(-30), "wrong");

            var currentIncId = IncrementId.ComposeId(startTime, "testing");
            var commitResult = await de.TryCommit(Keys((eid, wrongIncId)), EntIds(eid), currentIncId, default);
            Assert.True(commitResult.Error is Error.CommitError);

            await de.Flush();
            
            // check that actual value is not changed
            var retrieveResult = await de.Retrieve(eid, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(actualStamp, retrieveResult.Value);
            
            await AssertCommit(de, true, Keys((eid, actualIncId)),EntIds(eid), currentIncId);
        }

        [Fact]
        public async void PiSecWhenCacheNotEqualsTre() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = CreateEngine(repo, cache, tre, Logger());

            var startTime = DateTimeOffset.UtcNow.AddMinutes(-2);

            var eid = CreateRandomEntId();
            
            var originIncId = IncrementId.ComposeId(startTime, "testing");
            var originStamp = CreateSimpleStamp(originIncId, eid);
            await AssertCommit(de, true, EmptyKeys,EntIds(eid), originIncId);
            var uploadResult = await de.Upload(originStamp, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            // 1 minute letter
            // new increment went bad
            var actualIncId = IncrementId.ComposeId(startTime.AddMinutes(1), "testing");
            await AssertCommit(de, true, Keys((eid, originIncId)),EntIds(eid), actualIncId);
            // something went wrong and upload was not performed
            // ...
            // ...
            await de.Flush();
            
            // 1 minute latter...
            // new increment is performed
            var nextIncId = IncrementId.ComposeId(startTime.AddMinutes(2), "testing");

            var retrieveResult = await de.Retrieve(eid, default);
            Assert.True(retrieveResult.IsOk, retrieveResult.Error.ToString());
            Assert.Equal(originIncId, retrieveResult.Value.IncId);
            // origin incId in cache and actual incId is in transaction engine, so commit should fail
            await AssertCommit(de,false, Keys((eid, retrieveResult.Value.IncId)), EntIds(eid), nextIncId);

            await de.Flush();
            
            // piSec routine should set originValue in cache and transaction engine
            var retrieveResultAfterPiSec = await de.Retrieve(eid, default);
            Assert.True(retrieveResultAfterPiSec.IsOk, retrieveResultAfterPiSec.Error.ToString());
            Assert.Equal(originStamp, retrieveResultAfterPiSec.Value);
            
            // now next incId may be committed
            await AssertCommit(de, true, Keys((eid, retrieveResultAfterPiSec.Value.IncId)),EntIds(eid), nextIncId);
        }
        
        [Fact]
        public async void AppendNewEventTest() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var de = CreateEngine(repo, cache, tre, Logger());

            var incId = new IncrementId("100");
            var evt = CreateEvent(incId);
            var entId = CreateRandomEntId();
            var offsetId = CreateRandomEntId();
            
            var appendResult = await de.AppendEvent(entId, evt, offsetId, default);
            Assert.True(appendResult.IsOk, appendResult.Error.ToString());
            Assert.Equal(1, appendResult.Value);

            var cacheResult = await cache.GetEventLog(entId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            AssertEventsEqual(new[] {evt}, cacheResult.Value);

            await de.Flush();

            var repoResult = await repo.Load(entId.CreateStampKey(incId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            AssertEventsEqual(evt, new Event(incId, repoResult.Value.Data.Memory));
            
            var incId2 = new IncrementId("099");
            var evt2 = CreateEvent(incId2);
            var appendResult2 = await de.AppendEvent(entId, evt2, offsetId, default);
            Assert.True(appendResult2.IsOk, appendResult2.Error.ToString());
            Assert.Equal(2, appendResult2.Value);
            
            var cacheResult2 = await cache.GetEventLog(entId, default);
            Assert.True(cacheResult2.IsOk, cacheResult2.Error.ToString());
            AssertEventsEqual(new[] {evt, evt2}, cacheResult2.Value);

            await de.Flush();

            var repoResult2 = await repo.Load(entId.CreateStampKey(incId2), default);
            Assert.True(repoResult2.IsOk, repoResult2.Error.ToString());
            AssertEventsEqual(evt2, new Event(incId2, repoResult2.Value.Data.Memory));
        }

        [Fact]
        public async void AppendEventWithEntriesInRepo() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            
            var de = CreateEngine(repo, cache, tre, Logger());
            var entId = CreateRandomEntId();
            var offsetId = CreateRandomEntId();

            var events = new Event[5];
            for (var i = 0; i < events.Length; i++) {
                var incId = new IncrementId((100 - i).ToString("d4"));
                events[i] = CreateEvent(incId);
                var stamp = new BinaryStamp(entId.CreateStampKey(incId), new BinaryData("evt", events[i].Data));
                var saveResult = await repo.Save(stamp, default);
                Assert.True(saveResult.IsOk, saveResult.Error.ToString());
            }

            var newIncId = new IncrementId((100 - events.Length).ToString("d4"));
            var newEvt = CreateEvent(newIncId);
            var appendResult = await de.AppendEvent(entId, newEvt, offsetId, default);
            Assert.True(appendResult.IsOk, appendResult.Error.ToString());
            Assert.Equal(events.Length + 1, appendResult.Value);

            var cacheResult = await cache.GetEventLog(entId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            AssertEventsEqual(events.Append(newEvt).ToArray(), cacheResult.Value);

            await de.Flush();

            var repoResult = await repo.Load(entId.CreateStampKey(newIncId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            AssertEventsEqual(newEvt, new Event(newIncId, repoResult.Value.Data.Memory));
        }

        [Fact]
        public async void AppendEventWithEntriesAndOffsetInRepo() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            
            var de = CreateEngine(repo, cache, tre, Logger());
            var entId = CreateRandomEntId();
            var offsetId = CreateRandomEntId();

            var events = new Event[5];
            for (var i = 0; i < events.Length; i++) {
                var incId = new IncrementId((100 - i).ToString("d4"));
                events[i] = CreateEvent(incId);
                var stamp = new BinaryStamp(entId.CreateStampKey(incId), new BinaryData("evt", events[i].Data));
                var saveResult = await repo.Save(stamp, default);
                Assert.True(saveResult.IsOk, saveResult.Error.ToString());
            }

            var offsetData = Utils.EncodeOffset(events[2].IncId);
            var saveOffsetResult = await repo.Save(
                new BinaryStamp(offsetId.CreateStampKey(events[2].IncId), offsetData), default);
            Assert.True(saveOffsetResult.IsOk, saveOffsetResult.Error.ToString());

            var newIncId = new IncrementId((100 - events.Length).ToString("d4"));
            var newEvt = CreateEvent(newIncId);
            var appendResult = await de.AppendEvent(entId, newEvt, offsetId, default);
            Assert.True(appendResult.IsOk, appendResult.Error.ToString());
            Assert.Equal(events.Length - 3 + 1, appendResult.Value);

            var cacheResult = await cache.GetEventLog(entId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            AssertEventsEqual(events.Skip(3).Append(newEvt).ToArray(), cacheResult.Value);

            await de.Flush();

            var repoResult = await repo.Load(entId.CreateStampKey(newIncId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            AssertEventsEqual(newEvt, new Event(newIncId, repoResult.Value.Data.Memory));
        }

    }
}