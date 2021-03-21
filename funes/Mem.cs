using System;
using System.Collections.Generic;

namespace Funes {
    public readonly struct Mem<T> {

        public MemKey Key { get; }
        public IReadOnlyDictionary<string,string>? Headers { get; }
        public T Content { get; }

        public Mem(MemKey key, IReadOnlyDictionary<string,string>? headers, T content) {
            Key = key;
            Headers = headers;
            Content = content;
        }
    }
}