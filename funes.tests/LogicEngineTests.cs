using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Runtime.InteropServices;
using Funes.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Tests {
    
    public class LogicEngineTests {

        private readonly ITestOutputHelper _testOutputHelper;

        public LogicEngineTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        private LogicEngine<TModel, TMsg, TSideEffect> CreateLogicEngine<TModel, TMsg, TSideEffect>(
            ILogic<TModel, TMsg, TSideEffect> logic, IRepository? repository = null) {
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var tracer = new XUnitTracer<TModel, TMsg, TSideEffect>(_testOutputHelper);
            var ser = new SimpleSerializer<Simple>();
            var repo = repository ?? new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            return new LogicEngine<TModel, TMsg, TSideEffect>(logic, ser, de, logger, tracer);
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
            var logicEngine = CreateLogicEngine(new BasicLogic());

            var fact = new Entity(new EntityId("/tests/fact"), "World");
            var result = await logicEngine.Run(fact, null!, default);
            Assert.True(result.IsOk);
            Assert.Equal("Publish: Hello, World", result.Value.SideEffects.First());
        }
        
        public class AdvanceLogic : ILogic<string, string, string> {
            public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) {
                var n = (int) fact.Value;
                var commands = new List<Cmd<string,string>>();
                if (n % 2 == 0) commands.Add(new Cmd<string,string>.MsgCmd("Flip"));
                if (n % 3 == 0) commands.Add(new Cmd<string,string>.MsgCmd("Flop"));
                return ("", new Cmd<string, string>.BatchCmd(commands));
            }
            public (string, Cmd<string, string>) Update(string model, string msg) =>
                msg switch {
                    "Flip" => (model + "Flip", Cmd<string, string>.None),
                    "Flop" => (model + "Flop", Cmd<string, string>.None),
                    _ => ("???", Cmd<string, string>.None)
                };
            public Cmd<string, string>.OutputCmd End(string model) =>
                new Cmd<string, string>.BatchOutputCmd(new Cmd<string, string>.OutputCmd[] {
                    new Cmd<string, string>.SideEffectCmd("Publish: " + model),
                    new Cmd<string, string>.DerivedFactCmd(new Entity(new EntityId("/tests/advanced"), "42"))
                });
        }
        
        [Fact]
        public async void AdvancedLogicTest() {
            var logicEngine = CreateLogicEngine(new AdvanceLogic());

            var fact = new Entity(new EntityId("/tests/fact"), 6);
            var result = await logicEngine.Run(fact, null!, default);
            Assert.True(result.IsOk);
            Assert.Equal("Publish: FlipFlop", result.Value.SideEffects.First());
            Assert.Equal("42", result.Value.DerivedFacts.First().Value.Value.ToString());
        }
        
        public class RetrieveLogic : ILogic<string, string, string> {
            public static EntityId Eid => new EntityId("/tests/entity1");
            public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) {
                return ("", new Cmd<string, string>.RetrieveCmd(Eid, 
                    entry => (string)fact.Value + ((Simple)entry.Value).Value));
            }
            public (string, Cmd<string, string>) Update(string model, string msg) {
                var entity = new Entity(Eid,  new Simple(0, msg));
                return ("", new Cmd<string,string>.ConclusionCmd(entity));
            }
            public Cmd<string, string>.OutputCmd End(string model) => Cmd<string, string>.None;
        }
        
        [Fact]
        public async void RetrieveLogicTest() {
            var ser = new SimpleSerializer<Simple>();
            var repo = new SimpleRepository();
            var entity = new Entity(RetrieveLogic.Eid, new Simple(0, "42"));
            var saveResult = await repo.Save(new EntityStamp(entity, CognitionId.NewId()), ser, default);
            Assert.True(saveResult.IsOk);
            
            var logicEngine = CreateLogicEngine(new RetrieveLogic(), repo);

            var fact = new Entity(new EntityId("/tests/fact"), "Answer:");
            var result = await logicEngine.Run(fact, null!, default);
            Assert.True(result.IsOk);
            var output = (Simple)result.Value.Outputs.First().Value.Value;
            Assert.Equal("Answer:42", output.Value);
        }
        
    }
}