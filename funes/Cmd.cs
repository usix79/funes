using System;
using System.Collections.Generic;
using System.Text;
using Funes.Indexes;
using Funes.Sets;
using Microsoft.Extensions.Logging;

namespace Funes {
    public abstract record Cmd<TMsg,TSideEffect> {
        public record MsgCmd(TMsg Msg) : Cmd<TMsg,TSideEffect>;
        public record RetrieveCmd(EntityId EntityId, Func<EntityEntry, TMsg> Action, bool AsPremise = true) : Cmd<TMsg,TSideEffect>;
        public record RetrieveManyCmd(EntityId[] EntityIds, Func<ReadOnlyMemory<EntityEntry>, TMsg> Action, bool AsPremise = true) : Cmd<TMsg,TSideEffect>;
        public record RetrieveSetCmd(string SetName, Func<IReadOnlySet<string>, TMsg> Action) : Cmd<TMsg,TSideEffect>;
        public record BatchCmd(params Cmd<TMsg, TSideEffect>[] Items) : Cmd<TMsg, TSideEffect> {
            public override string ToString() {
                var txt = new StringBuilder("Batch[");
                foreach (var item in Items) txt.Append(item).Append(',');
                txt.Append("]");
                return txt.ToString();
            }
        };
        
        public abstract record OutputCmd : Cmd<TMsg,TSideEffect>;
        public record NoneCmd : OutputCmd;
        public static readonly NoneCmd None = new ();
        public record UploadCmd(Entity Entity) : OutputCmd;
        public record SetCmd(string SetName, SetOp.Kind Op, string Tag) : OutputCmd;
        public record IndexCmd(string IndexName, IndexOp.Kind Op, string Key, string Value) : OutputCmd;
        public record SideEffectCmd(TSideEffect SideEffect) : OutputCmd;
        public record ConstantCmd(string Name, string Value) : OutputCmd;

        public record BatchOutputCmd(params OutputCmd[] Items) : OutputCmd {
            public override string ToString() {
                var txt = new StringBuilder("OutputBatch[");
                foreach (var item in Items) txt.Append(item).Append(',');
                txt.Append(']');
                return txt.ToString();
            }
        }

        public record LogCmd(LogLevel Level, string Message, params object[] Args) : Cmd<TMsg, TSideEffect>; 

        public static Cmd<TMsg, TSideEffect> Error(string msg, params object[] args) => new LogCmd(LogLevel.Error, msg, args);
        public static Cmd<TMsg, TSideEffect> Warning(string msg, params object[] args) => new LogCmd(LogLevel.Warning, msg, args);
        public static Cmd<TMsg, TSideEffect> Information(string msg, params object[] args) => new LogCmd(LogLevel.Information, msg, args);
        public static Cmd<TMsg, TSideEffect> Debug(string msg, params object[] args) => new LogCmd(LogLevel.Debug, msg, args);

        // TODO: consider using object pool for each type of command
    }
}