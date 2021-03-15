using System;
using System.Collections.Specialized;
using System.IO;

namespace Funes {
    public class Mem {
        public MemKey Key { get; }
        public NameValueCollection Headers { get; }
        public byte[] Data { get; }

        public Mem(MemKey key, NameValueCollection? headers, byte[]? data) {
            Key = key;
            Headers = headers ?? new NameValueCollection();
            Data = data ?? Array.Empty<byte>();
        }
    }
}