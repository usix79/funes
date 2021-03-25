using System;
using System.Threading.Tasks;

namespace Funes {
    public interface ITracer<in TState,TMsg,TSideEffect> {
        ValueTask BeginResult(Entity fact, TState state, Cmd<TMsg,TSideEffect> cmd);
        ValueTask UpdateResult(TState state, Cmd<TMsg,TSideEffect> cmd);
        ValueTask EndResult(Cmd<TMsg,TSideEffect> cmd);
    }
}