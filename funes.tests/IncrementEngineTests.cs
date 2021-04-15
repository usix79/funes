using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Indexes;
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
            ITransactionEngine? tre_ = null) {
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var tracer = new XUnitTracer<TModel, TMsg, TSideEffect>(_testOutputHelper);
            var ser = new SimpleSerializer<Simple>();
            var repo = repository ?? new SimpleRepository();
            var cache = cache_ ?? new SimpleCache();
            var tre = tre_ ?? new SimpleTransactionEngine();
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            var logicEngine = new  LogicEngine<TModel, TMsg, TSideEffect>(logic, ser, de, logger, tracer);

            return new IncrementEngineEnv<TModel, TMsg, TSideEffect>(logicEngine, behavior, ser, de, logger);
        }
        
        [Fact]
        public async void EmptyIncrement() {
            var sysSer = new SystemSerializer();
            var repo = new SimpleRepository();
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", Cmd<string, string>.None),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            Task Behavior(IncrementId incId, string sideEffect, CancellationToken ct) => Task.CompletedTask;

            var env = CreateIncrementEngineEnv(logic, Behavior, repo);
            var fact = CreateSimpleFact(0, "");
            var factStamp = new EntityStamp(fact, IncrementId. NewStimulusId());
            var result = await IncrementEngine<string, string, string>.Run(env, factStamp, default);
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());
            var repoResult = await repo.Load(Increment.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Increment);
            if (repoResult.Value.Value is Increment increment) {
                Assert.Equal(result.Value, increment.Id);
                Assert.Equal(factStamp.Key, increment.FactKey);
                Assert.Empty(increment.Inputs);
                Assert.Empty(increment.Outputs);
                Assert.Empty(increment.Constants);
            }
        }
        
        [Fact]
        public async void BaseIncrement() {
            var sysSer = new SystemSerializer();
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
            
            Task Behavior(IncrementId incId, string se, CancellationToken ct) {
                sideEffect = se;
                return Task.CompletedTask;
            }

            var env = CreateIncrementEngineEnv(logic, Behavior, repo);
            var fact = CreateSimpleFact(1, "fact");
            var factStamp = new EntityStamp(fact, IncrementId. NewStimulusId());

            var result = await IncrementEngine<string, string, string>.Run(env, factStamp, default);
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());
            Assert.Equal(fact, initEntity);
            Assert.Equal("init", updateModel);
            Assert.Equal("msg", updateMsg);
            Assert.Equal("update", endModel);
            Assert.Equal("effect", sideEffect);
            
            var repoResult = await repo.Load(Increment.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Increment);
            if (repoResult.Value.Value is Increment increment) {
                Assert.Equal(result.Value, increment.Id);
                Assert.Equal(fact.Id, increment.FactKey.EntId);
                Assert.Empty(increment.Inputs);
                Assert.Empty(increment.Outputs);
                Assert.Empty(increment.Constants);
                Assert.Equal("effect\n", 
                    increment.Details.FirstOrDefault(pair => pair.Key == Increment.DetailsSideEffects).Value);
            }
        }

        [Fact]
        public async void IncrementWithConcurrency() {
            var sysSer = new SystemSerializer();
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();

            var evt = new ManualResetEventSlim(false);

            var eid = CreateRandomEntId();
            var stamp = new EntityStamp(new Entity(eid, new Simple(0, "value1")), IncrementId.NewId()); 
            Assert.True((await repo.Save(stamp, ser, default)).IsOk);

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

            Task Behavior(IncrementId incId, string sideEffect, CancellationToken ct) => Task.CompletedTask;

            var env1 = CreateIncrementEngineEnv(logic, Behavior, repo, cache, tre);
            var env2 = CreateIncrementEngineEnv(logicWithWaiting, Behavior, repo, cache, tre);
            var fact = CreateSimpleFact(0, "");
            var factStamp = new EntityStamp(fact, IncrementId. NewStimulusId());
            var fact2Stamp = new EntityStamp(fact, IncrementId. NewStimulusId());

            var waitingTask = Task.Factory.StartNew(() => IncrementEngine<string, string, string>.Run(env2, fact2Stamp)).Unwrap();
            
            var result1 = await IncrementEngine<string, string, string>.Run(env1, factStamp);
            Assert.True(result1.IsOk, result1.Error.ToString());
            evt.Set();

            var result2 = await waitingTask;
            Assert.True(result2.IsOk, result2.Error.ToString());

            await env1.DataEngine.Flush();
            await env2.DataEngine.Flush();
            
            var repoSuccessCognitionResult = await repo.Load(Increment.CreateStampKey(result2.Value), sysSer, default);
            Assert.True(repoSuccessCognitionResult.IsOk, repoSuccessCognitionResult.Error.ToString());
            Assert.True(repoSuccessCognitionResult.Value.Value is Increment);
            if (repoSuccessCognitionResult.Value.Value is Increment increment) {
                Assert.Equal(result2.Value, increment.Id);
                Assert.Equal(fact.Id, increment.FactKey.EntId);
                Assert.Equal(new List<KeyValuePair<EntityStampKey, bool>>
                    {new (eid.CreateStampKey(result1.Value), true)}, increment.Inputs);
                Assert.Equal(new EntityId[]{eid}, increment.Outputs);
                Assert.Empty(increment.Constants);
                Assert.Equal("2", increment.FindDetail(Increment.DetailsAttempt));
            }

            var childrenHistoryResult = await repo.HistoryBefore(Increment.CreateChildEntId(fact2Stamp.IncId),
                IncrementId.Singularity, 42, default);
            Assert.True(childrenHistoryResult.IsOk, childrenHistoryResult.Error.ToString());
            Assert.Equal(2, childrenHistoryResult.Value.Count());
            var childIncId = childrenHistoryResult.Value.First(x => !x.IsSuccess());

            var repoIncrementResult = await repo.Load(Increment.CreateStampKey(childIncId), sysSer, default);
            Assert.True(repoIncrementResult.IsOk, repoIncrementResult.Error.ToString());
            Assert.True(repoIncrementResult.Value.Value is Increment);
            if (repoIncrementResult.Value.Value is Increment childIncrement) {
                Assert.Equal(childIncId, childIncrement.Id);
                Assert.Equal(fact.Id, childIncrement.FactKey.EntId);
                Assert.Equal(new List<KeyValuePair<EntityStampKey, bool>>
                    {new (stamp.Key, true)}, childIncrement.Inputs);
                Assert.Equal(new EntityId[]{eid}, childIncrement.Outputs);
                Assert.Empty(childIncrement.Constants);
                Assert.Equal("1", childIncrement.FindDetail(Increment.DetailsAttempt));
            }
        }
        [Fact]
        public async void IncrementWithIndex() {
            var sysSer = new SystemSerializer();
            var repo = new SimpleRepository();
            var cache = new SimpleCache();

            var entId = CreateRandomEntId();
            var idxName = "testIdx";
            var tag = "top";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string, string>.TagCmd(idxName, entId.Id, tag )),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);


            Task Behavior(IncrementId incId, string se, CancellationToken ct) => Task.CompletedTask;

            var env = CreateIncrementEngineEnv(logic, Behavior, repo, cache);
            var fact = CreateSimpleFact(1, "fact");
            var factStamp = new EntityStamp(fact, IncrementId. NewStimulusId());

            var result = await IncrementEngine<string, string, string>.Run(env, factStamp, default);
            var incId = result.Value;
            await env.DataEngine.Flush();
            Assert.True(result.IsOk, result.Error.ToString());

            var indexRecordId = IndexHelpers.GetRecordId(idxName);
            var expectedOp = new IndexOp(IndexOp.Kind.AddTag, entId.Id, tag);

            var cacheResult = await cache.GetEventLog(indexRecordId, default);
            Assert.True(cacheResult.IsOk, cacheResult.Error.ToString());
            Assert.Equal(incId, cacheResult.Value.First);
            Assert.Equal(incId, cacheResult.Value.Last);
            var reader = new IndexRecordReader(cacheResult.Value.Data);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            Assert.False(reader.MoveNext());
            
            var repoResult = await repo.LoadEvent(indexRecordId.CreateStampKey(incId), default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            reader = new IndexRecordReader(repoResult.Value.Data);
            Assert.True(reader.MoveNext());
            Assert.Equal(expectedOp, reader.Current);
            
            var repoResultInc = await repo.Load(Increment.CreateStampKey(incId), sysSer, default);
            Assert.True(repoResultInc.IsOk, repoResultInc.Error.ToString());
            Assert.True(repoResultInc.Value.Value is Increment);
            if (repoResultInc.Value.Value is Increment increment) {
                Assert.Equal(new EntityId[]{indexRecordId}, increment.Outputs);
            }
        }
        
    }
}