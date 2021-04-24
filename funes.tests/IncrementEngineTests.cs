using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Indexes;
using Funes.Sets;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class IncrementEngineTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public IncrementEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        private IncrementEngineEnv<TModel, TMsg, TSideEffect> CreateIncrementEngineEnv<TModel, TMsg, TSideEffect>(
            ILogic<TModel, TMsg, TSideEffect> logic, Behavior<TSideEffect> behavior, 
            IRepository? repository = null,
            ICache? cache_ = null,
            ITransactionEngine? tre_ = null,
            int maxAttempts = 3,
            int maxEventLogSize = 42) {
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var tracer = new XUnitTracer<TModel, TMsg, TSideEffect>(_testOutputHelper);
            var ser = new SimpleSerializer<Simple>();
            var repo = repository ?? new SimpleRepository();
            var cache = cache_ ?? new SimpleCache();
            var tre = tre_ ?? new SimpleTransactionEngine();
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            var logicEngine = new  LogicEngineEnv<TModel, TMsg, TSideEffect>(logic, ser, de, logger, tracer);

            return new IncrementEngineEnv<TModel, TMsg, TSideEffect>(logicEngine, behavior, ser, de, logger, maxAttempts, maxEventLogSize);
        }

        private async ValueTask<Increment> LoadIncrement(IRepository repo, IncrementId incId) {
            var repoResult = await repo.Load(Increment.CreateStampKey(incId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            var decodeResult = Increment.Decode(repoResult.Value.Data);
            Assert.True(decodeResult.IsOk, decodeResult.Error.ToString());
            return decodeResult.Value;
        }

        private async ValueTask<IncrementId> LoadOffset(IRepository repo, EntityId offsetId, IncrementId incId) {
            var loadOffsetResult = await repo.Load(offsetId.CreateStampKey(incId), default);
            Assert.True(loadOffsetResult.IsOk, loadOffsetResult.Error.ToString());
            var offset = new EventOffset(loadOffsetResult.Value.Data);
            return offset.GetLastIncId();
        }
        
        [Fact]
        public async void EmptyIncrement() {
            var repo = new SimpleRepository();
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", Cmd<string, string>.None),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo);
            var fact = CreateSimpleFact(0, "");
            var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());
            var result = await IncrementEngine<string, string, string>.Run(env, factEntry, default);
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());
            var increment = await LoadIncrement(repo, result.Value);
            Assert.Equal(result.Value, increment.Id);
            Assert.Equal(factEntry.Key, increment.FactKey);
            Assert.Empty(increment.Args.Entities);
            Assert.Empty(increment.Args.Events);
            Assert.Empty(increment.Outputs);
            Assert.Empty(increment.Constants);
        }
        
        [Fact]
        public async void BaseIncrement() {
            var repo = new SimpleRepository();

            Entity? initEntity = null;
            string updateModel = "";
            string updateMsg = "";
            string endModel = "";
            string sideEffect = "";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => {
                    initEntity = entity;
                    return ("init", new Cmd<string, string>.MsgCmd("msg"));
                },
                (model, msg) => {
                    (updateModel, updateMsg) = (model, msg);
                    return ("update", Cmd<string, string>.None);
                },
                model => {
                    endModel = model;
                    return new Cmd<string, string>.SideEffectCmd("effect");
                });
            
            ValueTask<Result<Void>> Behavior(IncrementId incId, string se, CancellationToken ct) {
                sideEffect = se;
                return ValueTask.FromResult(new Result<Void>(Void.Value));
            }

            var env = CreateIncrementEngineEnv(logic, Behavior, repo);
            var fact = CreateSimpleFact(1, "fact");
            var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());

            var result = await IncrementEngine<string, string, string>.Run(env, factEntry, default);
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());
            Assert.Equal(fact, initEntity);
            Assert.Equal("init", updateModel);
            Assert.Equal("msg", updateMsg);
            Assert.Equal("update", endModel);
            Assert.Equal("effect", sideEffect);
            
            var increment = await LoadIncrement(repo, result.Value);
            Assert.Equal(result.Value, increment.Id);
            Assert.Equal(fact.Id, increment.FactKey.EntId);
            Assert.Empty(increment.Args.Entities);
            Assert.Empty(increment.Args.Events);
            Assert.Empty(increment.Outputs);
            Assert.Empty(increment.Constants);
            Assert.Equal("effect\n", 
                increment.Details.FirstOrDefault(pair => pair.Key == Increment.DetailsSideEffects).Value);
        }

        [Fact]
        public async void IncrementWithConcurrency() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var evt = new ManualResetEventSlim(false);

            var eid = CreateRandomEntId();
            var stamp = CreateSimpleStamp(IncrementId.NewId(), eid);
            Assert.True((await repo.Save(stamp, default)).IsOk);

            var logic = new CallbackLogic<string,string,string>(
                fact => ("", new Cmd<string, string>.RetrieveCmd(eid, entry => "go", false)),
                (model, msg) => ("", new Cmd<string, string>.UploadCmd(new Entity(eid, new Simple(1, "value2")))),
                model => Cmd<string, string>.None);

            var logicWithWaiting = new CallbackLogic<string, string, string>(
                fact => ("", new Cmd<string, string>.RetrieveCmd(eid, entry => "go")),
                (model, msg) => ("", new Cmd<string, string>.UploadCmd(new Entity(eid, new Simple(2, "value3")))),
                model => {
                    evt.Wait();
                    return Cmd<string, string>.None; });

            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env1 = CreateIncrementEngineEnv(logic, Behavior, repo, cache, tre);
            var env2 = CreateIncrementEngineEnv(logicWithWaiting, Behavior, repo, cache, tre);
            var fact = CreateSimpleFact(0, "");
            var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());
            var fact2Entry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());

            var waitingTask = Task.Factory.StartNew(() => IncrementEngine<string, string, string>.Run(env2, fact2Entry)).Unwrap();
            
            var result1 = await IncrementEngine<string, string, string>.Run(env1, factEntry);
            Assert.True(result1.IsOk, result1.Error.ToString());
            evt.Set();

            var result2 = await waitingTask;
            Assert.True(result2.IsOk, result2.Error.ToString());

            await env1.DataEngine.Flush();
            await env2.DataEngine.Flush();

            var increment = await LoadIncrement(repo, result2.Value);
            Assert.Equal(result2.Value, increment.Id);
            Assert.Equal(fact.Id, increment.FactKey.EntId);
            Assert.Equal(new List<IncrementArgs.InputEntityLink>
                {new (eid.CreateStampKey(result1.Value), true)}, increment.Args.Entities);
            Assert.Equal(new EntityId[]{eid}, increment.Outputs);
            Assert.Empty(increment.Constants);
            Assert.Equal("2", increment.FindDetail(Increment.DetailsAttempt));

            var childrenHistoryResult = await repo.HistoryBefore(Increment.CreateChildEntId(fact2Entry.IncId),
                IncrementId.Singularity, 42, default);
            Assert.True(childrenHistoryResult.IsOk, childrenHistoryResult.Error.ToString());
            Assert.Equal(2, childrenHistoryResult.Value.Count());
            var childIncId = childrenHistoryResult.Value.First(x => !x.IsSuccess());

            var childIncrement = await LoadIncrement(repo, childIncId);
            Assert.Equal(childIncId, childIncrement.Id);
            Assert.Equal(fact.Id, childIncrement.FactKey.EntId);
            Assert.Equal(new List<IncrementArgs.InputEntityLink>
                {new (stamp.Key, true)}, childIncrement.Args.Entities);
            Assert.Equal(new EntityId[]{eid}, childIncrement.Outputs);
            Assert.Empty(childIncrement.Constants);
            Assert.Equal("1", childIncrement.FindDetail(Increment.DetailsAttempt));
        }
        
        [Fact]
        public async void IncrementWithSets() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();

            var setName = CreateRandomEntId().GetName();
            var tag = "top";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string, string>.SetCmd(setName, SetOp.Kind.Add, tag )),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache);
            var fact = CreateSimpleFact(1, "fact");
            var factEntry = EntityEntry.Ok(fact, IncrementId. NewStimulusId());

            var result = await IncrementEngine<string, string, string>.Run(env, factEntry, default);
            var incId = result.Value;
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());

            var indexRecordId = SetsModule.GetRecordId(setName);
            var expectedOp = new SetOp(SetOp.Kind.Add, tag);

            var cacheResult = await cache.GetEventLog(indexRecordId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            Assert.Equal(incId, cacheResult.Value.First);
            Assert.Equal(incId, cacheResult.Value.Last);
            var reader = new SetRecord.Reader(cacheResult.Value.Memory);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            Assert.False(reader.MoveNext());
            
            var repoResult = await repo.Load(indexRecordId.CreateStampKey(incId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            reader = new SetRecord.Reader(repoResult.Value.Data.Memory);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            
            var increment = await LoadIncrement(repo, incId);
            Assert.Equal(new []{indexRecordId}, increment.Outputs);
        }

        [Fact]
        public async void IncrementWithRebuildingSets() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();

            var maxLogSize = 10;
            var setName = CreateRandomEntId().GetName();
            
            var commands = new Cmd<string, string>.SetCmd[] {
                new (setName, SetOp.Kind.Add, "tagBBB"),
                new (setName, SetOp.Kind.Add, "tagAAA"),
                new (setName, SetOp.Kind.Add, "tagBBB"),
                new (setName, SetOp.Kind.Clear, ""),
                new (setName, SetOp.Kind.ReplaceWith, "tagCCC"),
                new (setName, SetOp.Kind.Add, "tagAAA"),
                new (setName, SetOp.Kind.Add, "tagZZZ"),
                new (setName, SetOp.Kind.Del, "tagQQQ"),
                new (setName, SetOp.Kind.Add, "tagQQQ"),
                new (setName, SetOp.Kind.Del, "tagAAA"),
            };
            
            Assert.Equal(maxLogSize, commands.Length);
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", commands[int.Parse(((Simple)entity.Value).Value)]),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache, maxEventLogSize:maxLogSize);

            var incrementIds = new IncrementId [maxLogSize];
            
            for (var i = 0; i < commands.Length; i++) {
                var fact = CreateSimpleFact(i, i.ToString());
                var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());
                var result = await IncrementEngine<string, string, string>.Run(env, factEntry, default);
                Assert.True(result.IsOk, result.Error.ToString());
                incrementIds[i] = result.Value;
            }

            await env.DataEngine.Flush();
            
            var tagRecordId = SetsModule.GetRecordId(setName);

            // repo should contain 10 records, 1 offset and 2 keys
            for (var i = 0; i < incrementIds.Length; i++) {
                var loadEventResult = await repo.Load(tagRecordId.CreateStampKey(incrementIds[i]), default);
                Assert.True(loadEventResult.IsOk, loadEventResult.Error.ToString());
                var reader = new SetRecord.Reader(loadEventResult.Value.Data.Memory);
                Assert.True(reader.MoveNext());
                var cmd = commands[i];
                var expectedOp = new SetOp(cmd.Op, cmd.Tag);
                Assert.Equal(expectedOp, reader.Current);
            }

            var lastIncId = incrementIds[^1];
            
            // offset should be on last incId
            var offsetId = SetsModule.GetOffsetId(setName);
            var offset = await LoadOffset(repo, offsetId, lastIncId);
            Assert.Equal(lastIncId, offset);
            
            var snapshotId = SetsModule.GetSnapshotId(setName);
            var loadSnapshotResult = await repo.Load(snapshotId.CreateStampKey(lastIncId), default);
            Assert.True(loadSnapshotResult.IsOk, loadSnapshotResult.Error.ToString());
            var snapshot = new SetSnapshot(loadSnapshotResult.Value.Data);

            var set = snapshot.GetSet();
            Assert.Equal(3, set.Count);
            Assert.Contains("tagCCC", set);
            Assert.Contains("tagZZZ", set);
            Assert.Contains("tagQQQ", set);

            // eventlog in cache should be empty
            var cacheResult = await cache.GetEventLog(tagRecordId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            Assert.True(cacheResult.Value.IsEmpty);
            
            // increment should contain record, offset and snapshot in outputs
            var incId = lastIncId;
            var increment = await LoadIncrement(repo, incId);
            Assert.Equal(new HashSet<EntityId>{snapshotId, offsetId, SetsModule.GetRecordId(setName)}, 
                new HashSet<EntityId>(increment.Outputs));
        }

        [Fact]
        public async void IncrementWithRetrieveNonExistingSet() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var setName = CreateRandomEntId().GetName();

            IReadOnlySet<string>? retrievedSet = null;
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string,string>.RetrieveSetCmd(setName, set => {
                    retrievedSet = set;
                    return $"Got set {set}";
                })),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) =>
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache);
         
            var fact = CreateSimpleFact(1, "Fact1");
            var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());
            var result = await IncrementEngine<string, string, string>.Run(env, factEntry);
            Assert.True(result.IsOk, result.Error.ToString());
            
            Assert.NotNull(retrievedSet);
            Assert.Empty(retrievedSet!);
        }

        [Fact]
        public async void IncrementWithRetrieveExistingSet() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();

            var snapshotIncId = new IncrementId("100"); 
            var setName = CreateRandomEntId().GetName();

            var set = new HashSet<string> {"AAA", "a3131", "ZYAA-1234"};
            var snapshot = SetSnapshot.FromSet(set);
            var snapshotStamp = snapshot.CreateStamp(SetsModule.GetSnapshotId(setName), snapshotIncId);
            var saveSnapshotResult = await repo.Save(snapshotStamp,default);
            Assert.True(saveSnapshotResult.IsOk, saveSnapshotResult.Error.ToString());

            var offsetId = SetsModule.GetOffsetId(setName);
            var offset = new EventOffset(BinaryData.Empty).NextGen(snapshotIncId);
            var saveOffsetResult = await repo.Save(offset.CreateStamp(offsetId, snapshotIncId), default);
            Assert.True(saveOffsetResult.IsOk, saveOffsetResult.Error.ToString());

            var record = new SetRecord() {
                new (SetOp.Kind.Add, "BBB"),
                new (SetOp.Kind.Del, "AAA")
            };
            var recordData = SetRecord.Builder.EncodeRecord(record);
            var firstInc = new IncrementId("099");
            var evt = new Event(firstInc, recordData.Memory);
            var recordId = SetsModule.GetRecordId(setName);
            var updateEventResult =
                await cache.UpdateEventsIfNotExists(recordId, new[] {evt}, default);
            Assert.True(updateEventResult.IsOk, updateEventResult.Error.ToString());
            
            IReadOnlySet<string>? retrievedSet = null;
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string,string>.RetrieveSetCmd(setName, set => {
                    retrievedSet = set;
                    return $"Got set {set}";
                })),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache);
         
            var fact = CreateSimpleFact(1, "Fact1");
            var factEntry = EntityEntry.Ok(fact, IncrementId.NewStimulusId());
            var result = await IncrementEngine<string, string, string>.Run(env, factEntry);
            Assert.True(result.IsOk, result.Error.ToString());
            
            Assert.NotNull(retrievedSet);
            Assert.Equal(3, retrievedSet!.Count);
            Assert.Contains("BBB", retrievedSet);
            Assert.Contains("a3131", retrievedSet);
            Assert.Contains("ZYAA-1234", retrievedSet);

            var increment = await LoadIncrement(repo, result.Value);

            Assert.Equal(new HashSet<IncrementArgs.InputEntityLink> {new (snapshotStamp.Key, false),}, 
                new HashSet<IncrementArgs.InputEntityLink>(increment.Args.Entities));
            Assert.Equal(new List<IncrementArgs.InputEventLink> {
                    new (recordId, firstInc, firstInc),
                },
                increment.Args.Events
            );
        }
        
        [Fact]
        public async void IncrementWithIndexes() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();

            var indexName = "testIndex";
            var key = "key1";
            var val = "val-124";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string, string>.IndexCmd(indexName, IndexOp.Kind.Update, key, val )),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            ValueTask<Result<Void>> Behavior(IncrementId _, string se, CancellationToken ct) => 
                ValueTask.FromResult(new Result<Void>(Void.Value));

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache);
            var fact = CreateSimpleFact(1, "fact");
            var factEntry = EntityEntry.Ok(fact, IncrementId. NewStimulusId());

            var result = await IncrementEngine<string, string, string>.Run(env, factEntry, default);
            var incId = result.Value;
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());

            var indexRecordId = IndexesModule.GetRecordId(indexName);
            var expectedOp = new IndexOp(IndexOp.Kind.Update, key, val);

            var cacheResult = await cache.GetEventLog(indexRecordId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            Assert.Equal(incId, cacheResult.Value.First);
            Assert.Equal(incId, cacheResult.Value.Last);
            var reader = new IndexRecord.Reader(cacheResult.Value.Memory);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            Assert.False(reader.MoveNext());
            
            var repoResult = await repo.Load(indexRecordId.CreateStampKey(incId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            reader = new IndexRecord.Reader(repoResult.Value.Data.Memory);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            
            var increment = await LoadIncrement(repo, incId);
            Assert.Equal(new []{indexRecordId}, increment.Outputs);
        }

    }
}