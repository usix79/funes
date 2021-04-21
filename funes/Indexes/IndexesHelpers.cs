using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
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
                
                await Utils.Tasks.WhenAll(uploadTasks, results, ct);
            }
            finally {
                ArrayPool<ValueTask<Result<string>>>.Shared.Return(uploadTasksArr);
            }
        }
        
        // return indexName if the index needs updating of the pages
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

        private readonly struct PageOp : IComparable<PageOp> {
            public PageOp(EntityId pageId, short idx, Kind op, string key, string value) =>
                (PageId, Idx, Op, Key, Value) = (pageId, idx, op, key, value);

            public enum Kind:byte {Unknown = 0, InsertAfter = 1, RemoveAt = 2};
            public EntityId PageId { get; }
            public short Idx { get; }
            public Kind Op { get; }
            public string Key { get; }
            public string Value { get; }
            
            public int CompareTo(PageOp other) {
                var pageIdComparison = PageId.CompareTo(other.PageId);
                if (pageIdComparison != 0) return pageIdComparison;
                return Idx.CompareTo(other.Idx);
            }
        }

        public static async ValueTask<Result<List<IndexPage>>> UpdateIndex(
            DataSource ds, string indexName, EventLog log, CancellationToken ct) {
            
            // for each key leave only last op
            Dictionary<string, IndexOp> ops = new();
            var reader = new IndexRecordsReader(log.Memory);
            foreach (var op in reader) ops[op.Key] = op;

            var pageOps = new List<PageOp>(ops.Count);
            foreach (var op in ops.Values) {
                switch (op.OpKind) {
                    case IndexOp.Kind.Update:
                        var keyRes = await GetIndexKey(op.Key);
                        if (keyRes.IsError) return new Result<List<IndexPage>>(keyRes.Error);

                        var keyValue = keyRes.Value.GetValue();
                        if (keyValue == op.Value) continue;
                        
                        if (keyValue != "") {
                            var removeRes = await RemoveOp(op.Key, keyValue);
                            if (removeRes.IsError) return new Result<List<IndexPage>>(removeRes.Error);
                            if (removeRes.Value.HasValue) pageOps.Add(removeRes.Value.Value);
                        }

                        var insertRes = await InsertOp(op.Key, op.Value);
                        if (insertRes.IsError) return new Result<List<IndexPage>>(insertRes.Error);
                        pageOps.Add(insertRes.Value);
                        break;
                    case IndexOp.Kind.Remove:
                        keyRes = await GetIndexKey(op.Key);
                        if (keyRes.IsError) return new Result<List<IndexPage>>(keyRes.Error);

                        keyValue = keyRes.Value.GetValue();
                        if (keyValue != "") {
                            var removeRes = await RemoveOp(op.Key, keyValue);
                            if (removeRes.IsError) return new Result<List<IndexPage>>(removeRes.Error);
                            if (removeRes.Value.HasValue) pageOps.Add(removeRes.Value.Value);
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
                
            }
            
            throw new NotImplementedException();

            async ValueTask<Result<IndexKey>> GetIndexKey(string key) {
                var keyId = GetKeyId(indexName, key);
                var res = await ds.Retrieve(keyId, ct);
                return res.IsOk
                    ? new Result<IndexKey>(new IndexKey(res.Value.Data))
                    : new Result<IndexKey>(res.Error);
            }

            async ValueTask<Result<PageOp?>> RemoveOp(string key, string value) {
                throw new NotImplementedException();
            }

            async ValueTask<Result<PageOp>> InsertOp(string key, string value) {
                throw new NotImplementedException();
                    
            }

        }
        
    }
}