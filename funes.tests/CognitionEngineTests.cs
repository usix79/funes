using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class CognitionEngineTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public CognitionEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        private CognitionEngine<TModel, TMsg, TSideEffect> CreateCognitionEngine<TModel, TMsg, TSideEffect>(
            ILogic<TModel, TMsg, TSideEffect> logic, Behavior<TSideEffect> behavior, IRepository? repository = null) {
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var tracer = new XUnitTracer<TModel, TMsg, TSideEffect>(_testOutputHelper);
            var ser = new SimpleSerializer<Simple>();
            var repo = repository ?? new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            var logicEngine = new  LogicEngine<TModel, TMsg, TSideEffect>(logic, ser, de, logger, tracer);

            return new CognitionEngine<TModel, TMsg, TSideEffect>(logicEngine, behavior, ser, de, logger);
        }
        
        [Fact]
        public async void EmptyCognition() {
            var sysSer = new SystemSerializer();
            var repo = new SimpleRepository();
            var logic = new CallbackLogic(
                entity => ("", Cmd<string, string>.None),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            Task Behavior(string sideEffect, CancellationToken ct) => Task.CompletedTask;

            var cognitionEngine = CreateCognitionEngine(logic, Behavior, repo);
            var fact = CreateSimpleFact(0, "");

            var result = await cognitionEngine.Run(fact, default);
            Assert.True(result.IsOk, result.Error.ToString());
            var repoResult = await repo.Load(Cognition.CreateStampKey(result.Value), sysSer, default);
            Assert.True(repoResult.IsOk, repoResult.Error.ToString());
            Assert.True(repoResult.Value.Value is Cognition);
            if (repoResult.Value.Value is Cognition cognition) {
                Assert.Equal(result.Value, cognition.Id);
                Assert.Equal(CognitionId.None, cognition.ParentId);
                Assert.Equal(CognitionStatus.Truth, cognition.Status);
                Assert.Equal(fact.Id, cognition.Fact);
                Assert.Empty(cognition.Inputs);
                Assert.Empty(cognition.Outputs);
                Assert.Empty(cognition.Constants);
                Assert.Empty(cognition.SideEffects);
            }
        }
    }
}