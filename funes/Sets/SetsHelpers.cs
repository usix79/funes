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
        
        
        public static async ValueTask UploadSetRecords(IDataEngine de, int max,
            IncrementId incId, Dictionary<string,SetRecord> records, List<EntityId> outputs, 
            ArraySegment<Result<string>> results, CancellationToken ct) {
            var uploadTasksArr = ArrayPool<ValueTask<Result<string>>>.Shared.Rent(records.Count);
            var uploadTasks = new ArraySegment<ValueTask<Result<string>>>(uploadTasksArr, 0, records.Count);
            try {
                var idx = 0;
                foreach (var pair in records)
                    uploadTasks[idx++] = UploadSetRecord(de, ct, incId, pair.Key, pair.Value, max, outputs);
                
                await Utils.Tasks.WhenAll(uploadTasks, results, ct);
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

            var offsetId = GetOffsetId(setName);
            var offsetResult = await de.Retrieve(offsetId, ct);
            if (offsetResult.IsError) return new Result<Void>(offsetResult.Error);
            var offset = new EventOffset(offsetResult.Value.Data);
            argsCollector.RegisterEntity(offsetResult.Value.Key, true);

            var snapshotId = GetSnapshotId(setName);
            var snapshotResult = await de.Retrieve(snapshotId, ct);
            if (snapshotResult.IsError) return new Result<Void>(snapshotResult.Error);
            argsCollector.RegisterEntity(snapshotResult.Value.Key, false);
            var snapshot = new SetSnapshot(snapshotResult.Value.Data);

            var recordId = GetRecordId(setName);
            var eventLogResult = await de.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<Void>(eventLogResult.Error);

            var eventLog = eventLogResult.Value;
            argsCollector.RegisterEvent(recordId, eventLog.First, eventLog.Last);
            var reader = new SetRecordsReader(eventLog.Memory);
            var newSnapshot = UpdateSnapshot(snapshot, reader);

            // try commit 
            var premises = PremisesArr.Rent();
            var conclusions = ConclusionsArr.Rent();
            try {
                premises[0] = offsetResult.Value.Key;
                conclusions[0] = offsetId;
                var commitResult = await de.TryCommit(premises, conclusions, incId, ct);

                if (commitResult.IsError)
                    return new Result<Void>(commitResult.Error);
            }
            finally {
                PremisesArr.Return(premises);
                ConclusionsArr.Return(conclusions);
            }

            var uploadSnapshotResult = await de.Upload(newSnapshot.CreateStamp(snapshotId, incId), ct);
            if (uploadSnapshotResult.IsError) return new Result<Void>(uploadSnapshotResult.Error);

            var newOffset = offset.NextGen(eventLog.Last);
            var uploadOffsetResult = await de.Upload(newOffset.CreateStamp(offsetId, incId), ct);
            if (uploadOffsetResult.IsError) return new Result<Void>(uploadOffsetResult.Error);

            var truncateResult = await de.TruncateEvents(recordId, eventLog.Last, ct);
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

            var snapshot = new SetSnapshot(snapshotResult.Value.Data);

            var recordId = GetRecordId(setName);
            var offsetId = GetOffsetId(setName);
            var eventLogResult = await ds.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<SetSnapshot>(eventLogResult.Error);
            
            var eventLog = eventLogResult.Value;
            argsCollector.RegisterEvent(recordId, eventLog.First, eventLog.Last);

            return new Result<SetSnapshot>(UpdateSnapshot(snapshot, new SetRecordsReader(eventLog.Memory)));
        }

        public static SetSnapshot UpdateSnapshot(SetSnapshot snapshot, SetRecordsReader reader) {
            var set = snapshot.GetSet();
            while (reader.MoveNext()) {
                var op = reader.Current;
                switch (op.OpKind) {
                    case SetOp.Kind.Add:
                        set.Add(op.Tag);
                        break;
                    case SetOp.Kind.Clear:
                        set.Clear();
                        break;
                    case SetOp.Kind.Del:
                        set.Remove(op.Tag);
                        break;
                    case SetOp.Kind.ReplaceWith:
                        set.Clear();
                        set.Add(op.Tag);
                        break;
                    case SetOp.Kind.Unknown:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            return SetSnapshot.FromSet(set);
        }
        
    }
}