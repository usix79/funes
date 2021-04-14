using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace Funes.Indexes {
    
    public static class IndexHelpers {
        private readonly struct IndexDsc {
            public EntityId RecordId { get; }
            public EntityId OffsetId { get; }

            public IndexDsc(EntityId recId, EntityId offsetId) =>
                (RecordId, OffsetId) = (recId, offsetId);
        }

        private static readonly ConcurrentDictionary<string, IndexDsc> Descriptors = new ();

        private static IndexDsc GetDsc(string indexName) {
            if (!Descriptors.TryGetValue(indexName, out var dsc)) {
                dsc = new IndexDsc(
                    new EntityId($"funes/indexes/{indexName}/records"),
                    new EntityId($"funes/indexes/{indexName}/offset")
                );
                Descriptors[indexName] = dsc;
            }

            return dsc;
        }

        public static EntityId GetRecordId(string indexName) => GetDsc(indexName).RecordId;

        public static EntityId GetOffsetId(string indexName) => GetDsc(indexName).OffsetId;
        
        public static int CalcSize(IndexRecord rec) {
            var size = 0;
            foreach (var op in rec) {
                size += 3 + 2 * op.Key.Length + 2 * op.Tag.Length;
            }

            return size;
        }

        public static void Serialize(IndexRecord rec, Memory<byte> memory) {
            var idx = 0;
            var span = memory.Span;
            foreach (var op in rec) {
                span[idx++] = (byte)op.OpKind; 
                span[idx++] = (byte)op.Key.Length; 
                span[idx++] = (byte)op.Tag.Length; 
                WriteString(op.Key);
                WriteString(op.Tag);
            }
            
            void WriteString(string str) {
                var strSpan = memory.Span;
                foreach (var ch in str) {
                    var loByte = (byte) ch;
                    var hiByte = (byte)((uint) ch >> 8);
                    strSpan[idx++] = loByte;
                    strSpan[idx++] = hiByte; // little endian
                }
            }
        }
    }
}