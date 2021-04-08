using Microsoft.VisualBasic.CompilerServices;

namespace Funes.Indexes {
    
    public readonly struct IndexOp {
        public enum Kind { Unknown = 0, AddTag = 1, RemoveTag = 2, RemoveAllTags = 3, ReplaceAllTags = 4, }

        public Kind OpKind { get; }
        public string Key { get; }
        public string Tag { get; }

        public IndexOp(Kind kind, string key, string tag) => (OpKind, Key, Tag) = (kind, key, tag);
    }
}