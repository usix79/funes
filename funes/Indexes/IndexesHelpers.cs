using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Indexes {
    
    public static class IndexesHelpers {

        private readonly struct Dsc {
            public Dsc(EntityId recordId, EntityId offsetId, EntityId rootId, string keysCat) {
                RecordId = recordId;
                OffsetId = offsetId;
                RootId = rootId;
                KeysCat = keysCat;
            }

            public EntityId RecordId { get; }
            public EntityId OffsetId { get; }
            public EntityId RootId { get; }
            public string KeysCat { get; }
        }

        private static readonly ConcurrentDictionary<string, Dsc> Descriptors = new ();

        private static Dsc GetDsc(string idxName) {
            if (!Descriptors.TryGetValue(idxName, out var dsc)) {
                dsc = new Dsc(
                    new EntityId($"funes/indexes/{idxName}/records"),
                    new EntityId($"funes/indexes/{idxName}/offset"),
                    new EntityId($"funes/indexes/{idxName}/pages/0"),
                    $"funes/indexes/{idxName}/keys/"
                );
                Descriptors[idxName] = dsc;
            }
            return dsc;
        }

        public static EntityId GetRecordId(string idxName) => GetDsc(idxName).RecordId;
        public static EntityId GetOffsetId(string idxName) => GetDsc(idxName).OffsetId;
        public static EntityId GetRootId(string idxName) => GetDsc(idxName).RootId;
        public static EntityId GetKeyId(string idxName, string key) =>
            new (GetDsc(idxName).KeysCat, key);

        public static EntityId GetChildPageId(EntityId parenId, int childId) =>
            new (parenId.Id, childId.ToString());

        public static bool IsIndexPage(EntityId eid) => 
            eid.Id.StartsWith("funes/indexes/") && eid.Id.Contains("/pages/");
        
        public  static int CalcSize(IndexRecord rec) {
            var size = 0;
            foreach (var op in rec)
                size += 3 + 2 * op.Key.Length + 2 * op.Value.Length;
            return size;
        }

        public static BinaryData EncodeRecord(IndexRecord record) {
            var memory = new Memory<byte>(new byte[CalcSize(record)]);
            var idx = 0;
            foreach (var op in record) {
                Utils.Binary.WriteByte(memory, ref idx, (byte)op.OpKind);
                Utils.Binary.WriteByte(memory, ref idx, (byte)op.Key.Length);
                Utils.Binary.WriteByte(memory, ref idx, (byte)op.Value.Length);
                Utils.Binary.WriteString(memory, ref idx, op.Key);
                Utils.Binary.WriteString(memory, ref idx, op.Value);
            }

            return new BinaryData("bin", memory);
        }
        
        public static async ValueTask UploadRecords(IDataEngine de, int max,
            IncrementId incId, Dictionary<string,IndexRecord> records, List<EntityId> outputs, 
            ArraySegment<Result<string>> results, CancellationToken ct) {
            var uploadTasksArr = ArrayPool<ValueTask<Result<string>>>.Shared.Rent(records.Count);
            var uploadTasks = new ArraySegment<ValueTask<Result<string>>>(uploadTasksArr, 0, records.Count);
            try {
                var idx = 0;
                foreach (var pair in records)
                    uploadTasks[idx++] = UploadRecord(de, ct, incId, pair.Key, pair.Value, max, outputs);
                
                await Utils.WhenAll(uploadTasks, results, ct);
            }
            finally {
                ArrayPool<ValueTask<Result<string>>>.Shared.Return(uploadTasksArr);
            }
        }
        
        // return indexName if the set needs snapshot updating
        static async ValueTask<Result<string>> UploadRecord(IDataEngine de, CancellationToken ct, 
            IncrementId incId, string indexName, IndexRecord record, int max, List<EntityId> outputs) {
            var data = EncodeRecord(record);
            var evt = new Event(incId, data.Memory);

            var recordId = GetRecordId(indexName);
            outputs.Add(recordId);
            var result = await de.AppendEvent(recordId, evt, GetOffsetId(indexName), ct);
                
            return result.IsOk
                ? new Result<string>(result.Value >= max ? indexName : "") 
                : new Result<string>(result.Error); 
        }
    }
}