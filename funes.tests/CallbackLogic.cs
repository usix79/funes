using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace Funes.Tests {
    
    public class CallbackLogic : ILogic<string, string, string> {
        private readonly Func<Entity, (string, Cmd<string, string>)> _onBegin;
        private readonly Func<string, string, (string, Cmd<string, string>)> _onUpdate;
        private readonly Func<string, Cmd<string, string>.OutputCmd> _onEnd;

        public CallbackLogic(
            Func<Entity, (string, Cmd<string, string>)> onBegin,
            Func<string, string, (string, Cmd<string, string>)> onUpdate,
            Func<string, Cmd<string, string>.OutputCmd> onEnd) =>
            (_onBegin, _onUpdate, _onEnd) = (onBegin, onUpdate, onEnd);

        public (string, Cmd<string, string>) Begin(Entity fact, IConstants constants) => _onBegin(fact);


        public (string, Cmd<string, string>) Update(string model, string msg) => _onUpdate(model, msg);

        public Cmd<string, string>.OutputCmd End(string model) => _onEnd(model);
    }
}