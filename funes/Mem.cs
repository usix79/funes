using System.Collections.Specialized;
using System.IO;

namespace Funes {
    public class Mem {
        public MemKey Key { get; }
        public NameValueCollection Headers { get; }
        
        public Stream Data { get; }

        public Mem(MemKey key, NameValueCollection? headers, Stream? data) {
            Key = key;
            Headers = headers ?? new NameValueCollection();
            Data = data ?? Stream.Null;
        }
    }
}