using System.Threading.Tasks;
using Xunit.Abstractions;

namespace Funes.Tests {
    
    public class XUnitTracer<TModel,TMsg,TSideEffect> : ITracer<TModel,TMsg,TSideEffect> {
        private readonly ITestOutputHelper _testOutputHelper;

        public XUnitTracer(ITestOutputHelper testOutputHelper) => _testOutputHelper = testOutputHelper;
        public ValueTask BeginResult(Entity fact, TModel model, Cmd<TMsg, TSideEffect> cmd) {
            _testOutputHelper.WriteLine($"BEGIN Fact: {fact} => Model: {model} Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }

        public ValueTask UpdateResult(TMsg msg, TModel model, Cmd<TMsg, TSideEffect> cmd) {
            _testOutputHelper.WriteLine($"UPDATE Msg: {msg} => Model: {model} Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }

        public ValueTask EndResult(Cmd<TMsg, TSideEffect> cmd) {
            _testOutputHelper.WriteLine($"END => Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }
    }
}