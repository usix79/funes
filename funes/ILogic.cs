using System.Collections.Specialized;

namespace Funes {
    public interface ILogic<TState, TMsg, TSideEffect> {
        public (TState, Cmd<TMsg, TSideEffect>) Begin(Entity fact, NameValueCollection? constants);
        public (TState, Cmd<TMsg, TSideEffect>) Update(TState state, TMsg msg);
        public Cmd<TMsg, TSideEffect>.OutputCmd End(TState state);
    }
}