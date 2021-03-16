using System;
using System.Collections.Specialized;
using System.IO;

namespace Funes {
    public class Mem {
        public MemKey Key { get; }
        public NameValueCollection Headers { get; }
        public Stream Content { get; }

        public Mem(MemKey key, NameValueCollection? headers, Stream? content) {
            Key = key;
            Headers = headers ?? new NameValueCollection();
            Content = content ?? Stream.Null;
        }
    }
}