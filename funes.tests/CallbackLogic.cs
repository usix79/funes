using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Funes.Tests {
    
    public class CallbackLogic<TModel, TMsg, TSideEffect> : ILogic<TModel, TMsg, TSideEffect> {
        private readonly Func<Entity, (TModel, Cmd<TMsg, TSideEffect>)> _onBegin;
        private readonly Func<TModel, TMsg, (TModel, Cmd<TMsg, TSideEffect>)> _onUpdate;
        private readonly Func<TModel, Cmd<TMsg, TSideEffect>.OutputCmd> _onEnd;

        public CallbackLogic(
                Func<Entity, (TModel, Cmd<TMsg, TSideEffect>)> onBegin, 
                Func<TModel, TMsg, (TModel, Cmd<TMsg, TSideEffect>)> onUpdate, 
                Func<TModel, Cmd<TMsg, TSideEffect>.OutputCmd> onEnd) {
            _onBegin = onBegin;
            _onUpdate = onUpdate;
            _onEnd = onEnd;
        }

        public (TModel, Cmd<TMsg, TSideEffect>) Begin(Entity fact, IConstants constants) => _onBegin(fact);


        public (TModel, Cmd<TMsg, TSideEffect>) Update(TModel model, TMsg msg) => _onUpdate(model, msg);

        public Cmd<TMsg, TSideEffect>.OutputCmd End(TModel model) => _onEnd(model);
    }
}