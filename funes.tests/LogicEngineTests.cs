using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using Funes.Impl;
using Funes.Indexes;
using Funes.Sets;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    
    public class LogicEngineTests {

        private readonly ITestOutputHelper _testOutputHelper;

        public LogicEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        private (LogicEngineEnv<TModel, TMsg, TSideEffect>, DataContext) CreateEnv<TModel, TMsg, TSideEffect>(
            ILogic<TModel, TMsg, TSideEffect> logic, IRepository? repository = null) {
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var tracer = new XUnitTracer<TModel, TMsg, TSideEffect>(_testOutputHelper);
            var ser = new SimpleSerializer<Simple>();
            var repo = repository ?? new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            var env = new LogicEngineEnv<TModel, TMsg, TSideEffect>(logic, logger, tracer);
            var ctx = new DataContext(de, ser);
            return (env, ctx);
        }
    
        public class BasicLogic : ILogic<string, string, string> {
            public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) =>
                (fact.Value.ToString(), new Cmd<string,string>.MsgCmd("Say Hello"))!;

            public (string, Cmd<string, string>) Update(string model, string msg) =>
                msg switch {
                    "Say Hello" => ($"Hello, {model}", Cmd<string, string>.None),
                    _ => ("???", Cmd<string, string>.None)
                };
        
            public Cmd<string, string>.OutputCmd End(string model) =>
                new Cmd<string, string>.SideEffectCmd("Publish: " + model);
        }

        [Fact]
        public async void BasicLogicTest() {
            var (env, ctx) = CreateEnv(new BasicLogic());

            var fact = new Entity(new EntityId("/tests/fact"), "World");
            var result = await LogicEngine<string, string, string>.Run(env, ctx, fact, null!, default);
            Assert.True(result.IsOk);
            Assert.Equal("Publish: Hello, World", result.Value.SideEffects.First());
        }
        
        public class AdvanceLogic : ILogic<string, string, string> {
            public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) {
                var n = (int) fact.Value;
                var commands = new List<Cmd<string,string>>();
                if (n % 2 == 0) commands.Add(new Cmd<string,string>.MsgCmd("Flip"));
                if (n % 3 == 0) commands.Add(new Cmd<string,string>.MsgCmd("Flop"));
                return ("", new Cmd<string, string>.BatchCmd(commands.ToArray()));
            }
            public (string, Cmd<string, string>) Update(string model, string msg) =>
                msg switch {
                    "Flip" => (model + "Flip", Cmd<string, string>.None),
                    "Flop" => (model + "Flop", Cmd<string, string>.None),
                    _ => ("???", Cmd<string, string>.None)
                };
            public Cmd<string, string>.OutputCmd End(string model) =>
                new Cmd<string, string>.BatchOutputCmd(new Cmd<string, string>.OutputCmd[] {
                    new Cmd<string, string>.SideEffectCmd("Publish: " + model)
                });
        }
        
        [Fact]
        public async void AdvancedLogicTest() {
            var (env, ctx) = CreateEnv(new AdvanceLogic());

            var fact = new Entity(new EntityId("/tests/fact"), 6);
            var result = await LogicEngine<string, string, string>.Run(env, ctx, fact, null!, default);
            Assert.True(result.IsOk);
            Assert.Equal("Publish: FlipFlop", result.Value.SideEffects.First());
        }
        
        public class RetrieveLogic : ILogic<string, string, string> {
            public static EntityId EntId => new EntityId("/tests/entity1");
            public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) {
                return ("", new Cmd<string, string>.RetrieveCmd(EntId, 
                    entry => (string)fact.Value + ((Simple)entry.Value).Value, false));
            }
            public (string, Cmd<string, string>) Update(string model, string msg) {
                var entity = new Entity(EntId,  new Simple(0, msg));
                return ("", new Cmd<string,string>.UploadCmd(entity));
            }
            public Cmd<string, string>.OutputCmd End(string model) => Cmd<string, string>.None;
        }
        
        [Fact]
        public async void RetrieveLogicTest() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var entity = new Entity(RetrieveLogic.EntId, new Simple(0, "42"));
            var encodeResult = ser.Encode(entity.Id, entity.Value);
            Assert.True(encodeResult.IsOk, encodeResult.Error.ToString());
            var stamp = new BinaryStamp(entity.Id.CreateStampKey(IncrementId.NewId()), encodeResult.Value);
            var saveResult = await repo.Save(stamp, default);
            Assert.True(saveResult.IsOk);
            
            var (env, ctx) = CreateEnv(new RetrieveLogic(), repo);

            var fact = new Entity(new EntityId("/tests/fact"), "Answer:");
            var result = await LogicEngine<string, string, string>.Run(env, ctx, fact, null!, default);
            Assert.True(result.IsOk);
            var output = (Simple)result.Value.Entities.First().Value.Value;
            Assert.Equal("Answer:42", output.Value);
        }

        [Fact]
        public async void AddTagLogicTest() {
            var setName = CreateRandomEntId().GetName();
            var tag = "top";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string, string>.SetCmd(setName, SetOp.Kind.Add, tag )),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            var (env, ctx) = CreateEnv(logic);

            var fact = new Entity(new EntityId("/tests/fact"), "!");
            var result = await LogicEngine<string, string, string>.Run(env, ctx, fact, null!, default);
            Assert.True(result.IsOk);
            Assert.True(result.Value.SetRecords.TryGetValue(setName, out var idxRecord));
            Assert.True(idxRecord!.Count == 1);
            var op = idxRecord[0];
            Assert.Equal(SetOp.Kind.Add, op.OpKind);
            Assert.Equal(tag, op.Tag);
        }

        [Fact]
        public async void AddIndexLogicTest() {
            var indexName = "testIndex";
            var key = "key1";
            var val = "val";
            
            var logic = new CallbackLogic<string,string,string>(
                entity => ("", new Cmd<string, string>.IndexCmd(indexName, IndexOp.Kind.Update, key, val )),
                (model, msg) => ("", Cmd<string, string>.None),
                model => Cmd<string, string>.None);
            
            var (env, ctx) = CreateEnv(logic);

            var fact = new Entity(new EntityId("/tests/fact"), "!");
            var result = await LogicEngine<string, string, string>.Run(env, ctx, fact, null!, default);
            Assert.True(result.IsOk);
            Assert.True(result.Value.IndexRecords.TryGetValue(indexName, out var idxRecord));
            Assert.True(idxRecord!.Count == 1);
            var op = idxRecord[0];
            Assert.Equal(IndexOp.Kind.Update, op.OpKind);
            Assert.Equal(key, op.Key);
            Assert.Equal(val, op.Value);
        }

    }
}