using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Sets {
    
    public static class SetsHelpers {
        private readonly struct Dsc {
            public EntityId RecordId { get; }
            public EntityId OffsetId { get; }
            public EntityId SnapshotId { get; }

            public Dsc(EntityId recId, EntityId offsetId, EntityId snapshotId) => 
                (RecordId, OffsetId, SnapshotId) = (recId, offsetId, snapshotId);
        }

        private static readonly ConcurrentDictionary<string, Dsc> Descriptors = new ();

        private static Dsc GetDsc(string setName) {
            if (!Descriptors.TryGetValue(setName, out var dsc)) {
                dsc = new Dsc(
                    new EntityId($"funes/sets/{setName}/records"),
                    new EntityId($"funes/sets/{setName}/offset"),
                    new EntityId($"funes/sets/{setName}/snapshot")
                );
                Descriptors[setName] = dsc;
            }

            return dsc;
        }

        public static EntityId GetRecordId(string setName) => GetDsc(setName).RecordId;
        public static EntityId GetOffsetId(string setName) => GetDsc(setName).OffsetId;
        public static EntityId GetSnapshotId(string setName) => GetDsc(setName).SnapshotId;

        public static bool IsSnapshot(EntityId eid) => 
            eid.Id.StartsWith("funes/sets/") && eid.Id.EndsWith("/snapshot");
        
        
        public static int CalcSize(SetSnapshot aSnapshot) {
            var size = 4; // int count
            foreach (var tag in aSnapshot) {
                size += 1 + 2 * tag.Length;
            }
            return size;
        }

        public static BinaryData EncodeSnapshot(SetSnapshot snapshot) {
            var memory = new Memory<byte>(new byte[CalcSize(snapshot)]);
            var idx = 0;
            
            Utils.Binary.WriteInt32(memory, ref idx, snapshot.Count);
            foreach (var tag in snapshot) {
                memory.Span[idx++] = (byte)tag.Length;
                Utils.Binary.WriteString(memory, ref idx, tag);
            }

            return new BinaryData("bin", memory);
        }
        
        public static int CalcSize(SetRecord rec) {
            var size = 0;
            foreach (var op in rec)
                size += 2 + 2 * op.Tag.Length;
            return size;
        }

        public static BinaryData EncodeRecord(SetRecord setRecord) {
            var idx = 0;
            var memory = new Memory<byte>(new byte[CalcSize(setRecord)]);
            foreach (var op in setRecord) {
                Utils.Binary.WriteByte(memory, ref idx, (byte)op.OpKind);
                Utils.Binary.WriteByte(memory, ref idx, (byte)op.Tag.Length);
                Utils.Binary.WriteString(memory, ref idx, op.Tag);
            }

            return new BinaryData("bin", memory);
        }
        
        public static Result<SetSnapshot> DecodeSnapshot(BinaryData data) {
            if ("bin" != data.Encoding) return Result<SetSnapshot>.NotSupportedEncoding(data.Encoding);
            try {
                var idx = 0;
                var count = Utils.Binary.ReadInt32(data.Memory, ref idx);
                var snapshot = new SetSnapshot(count);

                while (idx < data.Memory.Length) {
                    var charsCount = Utils.Binary.ReadByte(data.Memory, ref idx);
                    var tag = charsCount > 0
                        ? Utils.Binary.ReadString(data.Memory, ref idx, charsCount)
                        : "";
                    snapshot.Add(tag);
                }
                return new Result<SetSnapshot>(snapshot);
            }
            catch (Exception e) {
                return Result<SetSnapshot>.Exception(e);
            }
        }
        
        public static async ValueTask UploadSetRecords(IDataEngine de, int max,
            IncrementId incId, Dictionary<string,SetRecord> records, List<EntityId> outputs, 
            ArraySegment<Result<string>> results, CancellationToken ct) {
            var uploadTasksArr = ArrayPool<ValueTask<Result<string>>>.Shared.Rent(records.Count);
            var uploadTasks = new ArraySegment<ValueTask<Result<string>>>(uploadTasksArr, 0, records.Count);
            try {
                var idx = 0;
                foreach (var pair in records)
                    uploadTasks[idx++] = UploadSetRecord(de, ct, incId, pair.Key, pair.Value, max, outputs);
                
                await Utils.WhenAll(uploadTasks, results, ct);
            }
            finally {
                ArrayPool<ValueTask<Result<string>>>.Shared.Return(uploadTasksArr);
            }
        }

        // return setName if the set needs snapshot updating
        static async ValueTask<Result<string>> UploadSetRecord(IDataEngine de, CancellationToken ct, 
            IncrementId incId, string setName, SetRecord record, int max, List<EntityId> outputs) {

            var data = EncodeRecord(record);
            var evt = new Event(incId, data.Memory);

            var recordId = GetRecordId(setName);
            outputs.Add(recordId);
            var result = await de.AppendEvent(recordId, evt, GetOffsetId(setName), ct);
                
            return result.IsOk
                ? new Result<string>(result.Value >= max ? setName : "") 
                : new Result<string>(result.Error); 
        }

        private static readonly Utils.ObjectPool<StampKey[]> PremisesArr = new (() => new StampKey [1], 7);
        private static readonly Utils.ObjectPool<EntityId[]> ConclusionsArr = new (() => new EntityId [1], 7);
        public static async Task<Result<Void>> UpdateSnapshot(IDataEngine de, IncrementId incId, string setName, 
            IIncrementArgsCollector argsCollector, List<EntityId> outputs, CancellationToken ct) {
            
            var snapshotId = GetSnapshotId(setName);
            var snapshotResult = await de.Retrieve(snapshotId, ct);
            if (snapshotResult.IsError) return new Result<Void>(snapshotResult.Error);
            argsCollector.RegisterEntity(snapshotResult.Value.Key, false);

            SetSnapshot snapshot;
            if (snapshotResult.Value.IsNotEmpty) {
                var decodeResult = DecodeSnapshot(snapshotResult.Value.Data);
                if (decodeResult.IsError) return new Result<Void>(decodeResult.Error);
                snapshot = decodeResult.Value;
            }
            else {
                snapshot = new SetSnapshot();
            }

            var recordId = GetRecordId(setName);
            var offsetId = GetOffsetId(setName);

            var eventLogResult = await de.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<Void>(eventLogResult.Error);

            var eventLog = eventLogResult.Value;
            argsCollector.RegisterEvent(recordId, eventLog.First, eventLog.Last);
            var reader = new SetRecordsReader(eventLog.Data);
            UpdateSnapshot(snapshot, reader);

            // try commit 
            var premises = PremisesArr.Rent();
            var conclusions = ConclusionsArr.Rent();
            try {
                premises[0] = snapshotResult.Value.Key;
                conclusions[0] = snapshotId;
                var commitResult = await de.TryCommit(premises, conclusions, incId, ct);

                if (commitResult.IsError)
                    return new Result<Void>(commitResult.Error);
            }
            finally {
                PremisesArr.Return(premises);
                ConclusionsArr.Return(conclusions);
            }

            var snapshotStamp = new BinaryStamp(snapshotId.CreateStampKey(incId), EncodeSnapshot(snapshot)); 
            var uploadSnapshotResult = await de.Upload(snapshotStamp, ct);
            if (uploadSnapshotResult.IsError) return new Result<Void>(uploadSnapshotResult.Error);

            var truncateResult = await de.TruncateEvents(recordId, offsetId.CreateStampKey(incId), eventLog.Last, ct);
            if (truncateResult.IsError) return new Result<Void>(truncateResult.Error);

            outputs.Add(snapshotId);
            outputs.Add(offsetId);

            return new Result<Void>(Void.Value);
        }

        public static async Task<Result<SetSnapshot>> RetrieveSnapshot(IDataSource ds, string setName, 
            IIncrementArgsCollector argsCollector, CancellationToken ct) {
            
            var snapshotResult = await ds.Retrieve(GetSnapshotId(setName), ct);
            if (snapshotResult.IsError) return new Result<SetSnapshot>(snapshotResult.Error);
            argsCollector.RegisterEntity(snapshotResult.Value.Key, false);

            var decodeResult = DecodeSnapshot(snapshotResult.Value.Data);
            if (decodeResult.IsError) return new Result<SetSnapshot>(decodeResult.Error);

            var snapshot = decodeResult.Value; 
            var recordId = GetRecordId(setName);
            var offsetId = GetOffsetId(setName);

            var eventLogResult = await ds.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<SetSnapshot>(eventLogResult.Error);
            
            var eventLog = eventLogResult.Value;
            argsCollector.RegisterEvent(recordId, eventLog.First, eventLog.Last);
            var reader = new SetRecordsReader(eventLog.Data);
            UpdateSnapshot(snapshot, reader);

            return new Result<SetSnapshot>(snapshot);
        }

        public static void UpdateSnapshot(SetSnapshot snapshot, SetRecordsReader reader) {
            while (reader.MoveNext()) {
                var op = reader.Current;
                switch (op.OpKind) {
                    case SetOp.Kind.Add:
                        snapshot.Add(op.Tag);
                        break;
                    case SetOp.Kind.Clear:
                        snapshot.Clear();
                        break;
                    case SetOp.Kind.Del:
                        snapshot.Remove(op.Tag);
                        break;
                    case SetOp.Kind.ReplaceWith:
                        snapshot.Clear();
                        snapshot.Add(op.Tag);
                        break;
                    case SetOp.Kind.Unknown:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }
        
    }
}