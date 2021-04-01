using System;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class SimpleTracer<TModel,TMsg,TSideEffect> : ITracer<TModel,TMsg,TSideEffect> {
        public ValueTask BeginResult(Entity fact, TModel model, Cmd<TMsg, TSideEffect> cmd) {
            Console.WriteLine($"BEGIN Fact: {fact} => Model: {model} Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }

        public ValueTask UpdateResult(TMsg msg, TModel model, Cmd<TMsg, TSideEffect> cmd) {
            Console.WriteLine($"UPDATE Msg: {msg} => Model: {model} Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }

        public ValueTask EndResult(Cmd<TMsg, TSideEffect> cmd) {
            Console.WriteLine($"END => Cmd: {cmd}");
            return ValueTask.CompletedTask;
        }
    }
}