using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Sets {
    
    public static class SetsModule {
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

        public static bool IsSetSnapshot(EntityId eid) => 
            eid.Id.StartsWith("funes/sets/") && eid.Id.EndsWith("/snapshot");
        
        public static async ValueTask<Result<int>> UploadSetRecord(IDataEngine de, CancellationToken ct, 
            IncrementId incId, string setName, SetRecord record, List<EntityId> outputs) {

            var data = SetRecord.Builder.EncodeRecord(record);
            var evt = new Event(incId, data.Memory);

            var recordId = GetRecordId(setName);
            outputs.Add(recordId);
            return await de.AppendEvent(recordId, evt, GetOffsetId(setName), ct);
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
            var reader = new SetRecord.Reader(eventLog.Memory);
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

            return new Result<SetSnapshot>(UpdateSnapshot(snapshot, new SetRecord.Reader(eventLog.Memory)));
        }

        public static SetSnapshot UpdateSnapshot(SetSnapshot snapshot, SetRecord.Reader reader) {
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