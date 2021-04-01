using System;
using System.Threading.Tasks;

namespace Funes {
    public interface ITracer<in TModel,TMsg,TSideEffect> {
        ValueTask BeginResult(Entity fact, TModel model, Cmd<TMsg,TSideEffect> cmd);
        ValueTask UpdateResult(TMsg msg, TModel model, Cmd<TMsg,TSideEffect> cmd);
        ValueTask EndResult(Cmd<TMsg,TSideEffect> cmd);
    }
}