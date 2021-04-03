using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class IncrementEngineTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public IncrementEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        private IncrementEngine<TModel, TMsg, TSideEffect> CreateCognitionEngine<TModel, TMsg, TSideEffect>(
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

            return new IncrementEngine<TModel, TMsg, TSideEffect>(logicEngine, behavior, ser, de, logger);
        }
        
        [Fact]
        public async void EmptyCognition() {
            var sysSer = new SystemSerializer();
            var repo = new SimpleRepository();
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", Cmd<string, string>.None),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            Task Behavior(string sideEffect, CancellationToken ct) => Task.CompletedTask;

            var cognitionEngine = CreateCognitionEngine(logic, Behavior, repo);
            var fact = CreateSimpleFact(0, "");

            var result = await cognitionEngine.Run(fact, default);
            Assert.True(result.IsOk, result.Error.ToString());
            var repoResult = await repo.Load(Increment.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Increment);
            if (repoResult.Value.Value is Increment cognition) {
                Assert.Equal(result.Value, cognition.Id);
                Assert.Equal(IncrementId.None, cognition.ParentId);
                Assert.Equal(IncrementStatus.Success, cognition.Status);
                Assert.Equal(fact.Id, cognition.Fact);
                Assert.Empty(cognition.Inputs);
                Assert.Empty(cognition.Outputs);
                Assert.Empty(cognition.Constants);
                Assert.Empty(cognition.SideEffects);
            }
        }
        
        [Fact]
        public async void BaseCognition() {
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
            
            Task Behavior(string se, CancellationToken ct) {
                sideEffect = se;
                return Task.CompletedTask;
            }

            var cognitionEngine = CreateCognitionEngine(logic, Behavior, repo);
            var fact = CreateSimpleFact(1, "fact");

            var result = await cognitionEngine.Run(fact, default);
            Assert.True(result.IsOk, result.Error.ToString());
            Assert.Equal(fact, initEntity);
            Assert.Equal("init", updateModel);
            Assert.Equal("msg", updateMsg);
            Assert.Equal("update", endModel);
            Assert.Equal("effect", sideEffect);
            
            var repoResult = await repo.Load(Increment.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Increment);
            if (repoResult.Value.Value is Increment cognition) {
                Assert.Equal(result.Value, cognition.Id);
                Assert.Equal(IncrementId.None, cognition.ParentId);
                Assert.Equal(IncrementStatus.Success, cognition.Status);
                Assert.Equal(fact.Id, cognition.Fact);
                Assert.Empty(cognition.Inputs);
                Assert.Empty(cognition.Outputs);
                Assert.Empty(cognition.Constants);
                Assert.Equal(new List<string>{"effect"}, cognition.SideEffects);
            }
        }

        [Fact]
        public async void CognitionWithConcurrency() {
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

            Task Behavior(string sideEffect, CancellationToken ct) => Task.CompletedTask;

            var cognitionEngine1 = CreateCognitionEngine(logic, Behavior, repo, cache, tre);
            var cognitionEngine2 = CreateCognitionEngine(logicWithWaiting, Behavior, repo, cache, tre);
            var fact = CreateSimpleFact(0, "");

            var waitingTask = Task.Factory.StartNew(() => cognitionEngine2.Run(fact)).Unwrap();
            
            var result1 = await cognitionEngine1.Run(fact);
            Assert.True(result1.IsOk, result1.Error.ToString());
            evt.Set();

            var result2 = await waitingTask;
            Assert.True(result2.IsOk, result2.Error.ToString());
            
            var repoParentCognitionResult = await repo.Load(Increment.CreateStampKey(result2.Value), sysSer, default);
            Assert.True(repoParentCognitionResult.IsOk, repoParentCognitionResult.Error.ToString());
            Assert.True(repoParentCognitionResult.Value.Value is Increment);
            if (repoParentCognitionResult.Value.Value is Increment cognition) {
                Assert.Equal(result2.Value, cognition.Id);
                Assert.Equal(IncrementId.None, cognition.ParentId);
                Assert.Equal(IncrementStatus.Fail, cognition.Status);
                Assert.Equal(fact.Id, cognition.Fact);
                Assert.Equal(new List<KeyValuePair<EntityStampKey, bool>>
                    {new (stamp.Key, true)}, cognition.Inputs);
                Assert.Equal(new EntityId[]{eid}, cognition.Outputs);
                Assert.Empty(cognition.Constants);
                Assert.Empty(cognition.SideEffects);
                Assert.Equal("1", cognition.Details[Increment.DetailsAttempt]);
            }

            var childrenHistoryResult = await repo.History(Increment.CreateChildEntId(result2.Value),
                IncrementId.Singularity, 42, default);
            Assert.True(childrenHistoryResult.IsOk, childrenHistoryResult.Error.ToString());
            Assert.Single(childrenHistoryResult.Value);
            var childIncId = childrenHistoryResult.Value.First();

            var repoCognitionResult = await repo.Load(Increment.CreateStampKey(childIncId), sysSer, default);
            Assert.True(repoCognitionResult.IsOk, repoCognitionResult.Error.ToString());
            Assert.True(repoCognitionResult.Value.Value is Increment);
            if (repoCognitionResult.Value.Value is Increment childCognition) {
                Assert.Equal(childIncId, childCognition.Id);
                Assert.Equal(result2.Value, childCognition.ParentId);
                Assert.Equal(IncrementStatus.Success, childCognition.Status);
                Assert.Equal(fact.Id, childCognition.Fact);
                Assert.Equal(new List<KeyValuePair<EntityStampKey, bool>>
                    {new (eid.CreateStampKey(result1.Value), true)}, childCognition.Inputs);
                Assert.Equal(new EntityId[]{eid}, childCognition.Outputs);
                Assert.Empty(childCognition.Constants);
                Assert.Empty(childCognition.SideEffects);
                Assert.Equal("2", childCognition.Details[Increment.DetailsAttempt]);
            }
        }
        
        [Fact]
        public async void CognitionWithChildren() {
            var sysSer = new SystemSerializer();
            var repo = new SimpleRepository();

            var logic = new CallbackLogic<string, string, string>(
                entity => ("init", 
                    ((Simple)entity.Value).Id == 1
                        ? new Cmd<string,string>.BatchCmd(new [] {
                            new Cmd<string, string>.DerivedFactCmd(CreateSimpleFact(11, "fact1")),
                            new Cmd<string, string>.DerivedFactCmd(CreateSimpleFact(12, "fact2")),
                            new Cmd<string, string>.DerivedFactCmd(CreateSimpleFact(13, "fact3"))})
                        : Cmd<string,string>.None),
                (model, msg) => ("update", Cmd<string, string>.None),
                model => Cmd<string, string>.None);

            Task Behavior(string se, CancellationToken ct) => Task.CompletedTask;

            var cognitionEngine = CreateCognitionEngine(logic, Behavior, repo);
            var fact = CreateSimpleFact(1, "fact");

            var result = await cognitionEngine.Run(fact, default);
            Assert.True(result.IsOk, result.Error.ToString());
            
            var repoResult = await repo.Load(Increment.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Increment);
            if (repoResult.Value.Value is Increment cognition) {
                Assert.Equal(result.Value, cognition.Id);
                Assert.Equal(IncrementId.None, cognition.ParentId);
                Assert.Equal(IncrementStatus.Success, cognition.Status);
                Assert.Equal(fact.Id, cognition.Fact);
                Assert.Empty(cognition.Inputs);
                Assert.Empty(cognition.Outputs);
                Assert.Empty(cognition.Constants);
                Assert.Empty(cognition.SideEffects);
            }
            
            var childrenHistoryResult = await repo.History(Increment.CreateChildEntId(result.Value),
                IncrementId.Singularity, 42, default);
            Assert.True(childrenHistoryResult.IsOk, childrenHistoryResult.Error.ToString());
            Assert.Equal(3, childrenHistoryResult.Value.Count());
            foreach (var childIncId in childrenHistoryResult.Value) {
                var repoCognitionResult = await repo.Load(Increment.CreateStampKey(childIncId), sysSer, default);
                Assert.True(repoCognitionResult.IsOk, repoCognitionResult.Error.ToString());
                Assert.True(repoCognitionResult.Value.Value is Increment);
                if (repoCognitionResult.Value.Value is Increment childCognition) {
                    Assert.Equal(childIncId, childCognition.Id);
                    Assert.Equal(result.Value, childCognition.ParentId);
                    Assert.Equal(IncrementStatus.Success, childCognition.Status);
                    //Assert.Equal(fact.Id, childCognition.Fact);
                    Assert.Empty(childCognition.Inputs);
                    Assert.Empty(childCognition.Outputs);
                    Assert.Empty(childCognition.Constants);
                    Assert.Empty(childCognition.SideEffects);
                }
            }
            
        }
        
    }
}