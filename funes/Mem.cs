using System;
using System.Collections.Generic;

namespace Funes {
    public readonly struct Mem<T> {

        public MemId Id { get; }
        public IReadOnlyDictionary<string,string>? Headers { get; }
        public T Content { get; }

        public Mem(MemId id, IReadOnlyDictionary<string,string>? headers, T content) {
            Id = id;
            Headers = headers;
            Content = content;
        }
    }
}