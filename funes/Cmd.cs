using System;
using System.Collections.Generic;

namespace Funes {
    public abstract record Cmd<TMsg,TSideEffect> {
        public record NoneCmd : Cmd<TMsg,TSideEffect>;
        public record MsgCmd(TMsg Msg) : Cmd<TMsg,TSideEffect>;
        public record RetrieveCmd(EntityId EntityId, Func<EntityEntry, TMsg> Action) : Cmd<TMsg,TSideEffect>;
        public record RetrieveManyCmd(IEnumerable<EntityId> MemIds, Func<EntityEntry[], TMsg> Action) : Cmd<TMsg,TSideEffect>;
        public record BatchCmd(IEnumerable<Cmd<TMsg,TSideEffect>> Items) : Cmd<TMsg,TSideEffect>;
        public abstract record OutputCmd : Cmd<TMsg,TSideEffect>;
        public record ConclusionCmd(Entity Entity) : OutputCmd;
        public record DerivedFactCmd(Entity Entity) : OutputCmd;
        public record SideEffectCmd(TSideEffect SideEffect) : OutputCmd;
        public record BatchOutputCmd(IEnumerable<OutputCmd> Items) : OutputCmd;

        public static readonly Cmd<TMsg,TSideEffect> None = new NoneCmd();
        
        // TODO: consider private constructors and static creators
    }
}