using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Funes {
    public interface ILogic<TModel, TMsg, TSideEffect> {
        public (TModel, Cmd<TMsg, TSideEffect>) Begin(Entity fact, IConstants constants);
        public (TModel, Cmd<TMsg, TSideEffect>) Update(TModel model, TMsg msg);
        public Cmd<TMsg, TSideEffect>.OutputCmd End(TModel model);
    }
}