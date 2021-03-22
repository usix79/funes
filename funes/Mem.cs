using System;

namespace Funes {
    public readonly struct Mem {

        public MemKey Key { get; }
        public object Value { get; }

        public Mem(MemKey key,  object value) {
            Key = key;
            Value = value;
        }
    }
}