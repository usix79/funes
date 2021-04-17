using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
            foreach (var op in rec) {
                size += 2 + 2 * op.Tag.Length;
            }
            return size;
        }

        public static void SerializeRecord(SetRecord rec, Memory<byte> memory) {
            var idx = 0;
            var span = memory.Span;
            foreach (var op in rec) {
                span[idx++] = (byte)op.OpKind; 
                span[idx++] = (byte)op.Tag.Length; 
                WriteString(op.Tag);
            }
            
            void WriteString(string str) {
                var strSpan = memory.Span;
                foreach (var ch in str) {
                    var (loByte, hiByte) = Utils.CharToUtf16Bytes(ch);
                    strSpan[idx++] = loByte;
                    strSpan[idx++] = hiByte; // little endian
                }
            }
        }
        
        public static async ValueTask<Result<string>> EncodeJson(Stream output, SetSnapshot snapshot) {
            try {
                await JsonSerializer.SerializeAsync(output, snapshot);
                return new Result<string>("json");
            }
            catch (Exception e) {
                return Result<string>.SerdeError(e.Message);
            }
        }

        public static async ValueTask<Result<object>> DecodeJson(Stream input, string encoding) {
            if ("json" != encoding) return Result<object>.NotSupportedEncoding(encoding);
            try {
                var reflectionOrNull = await JsonSerializer.DeserializeAsync<SetSnapshot>(input);
                return reflectionOrNull != null
                    ? new Result<object>(reflectionOrNull)
                    : Result<object>.SerdeError("null");
            }
            catch (Exception e) {
                return Result<object>.SerdeError(e.Message);
            }
        }

        public static ValueTask<Result<string>> Encode(Stream output, SetSnapshot snapshot) {
            try {
                var count = snapshot.Count;
                output.WriteByte((byte) count);
                output.WriteByte((byte) (count >> 8));
                output.WriteByte((byte) (count >> 16));
                output.WriteByte((byte) (count >> 24));

                foreach (var tag in snapshot) {
                    output.WriteByte((byte)tag.Length);
                    foreach (var ch in tag) {
                        var (loByte, hiByte) = Utils.CharToUtf16Bytes(ch);
                        output.WriteByte(loByte);
                        output.WriteByte(hiByte);
                    }
                }
                return ValueTask.FromResult(new Result<string>("bin"));
            }
            catch (Exception e) {
                return ValueTask.FromResult(Result<string>.SerdeError(e.Message));
            }
        }

        public static async ValueTask<Result<object>> Decode(Stream input, string encoding, CancellationToken ct) {
            if ("bin" != encoding) return Result<object>.NotSupportedEncoding(encoding);
            try {
                var count = input.ReadByte();
                count |= input.ReadByte() << 8;
                count |= input.ReadByte() << 16;
                count |= input.ReadByte() << 24;

                var snapshot = new SetSnapshot(count);

                var len = input.ReadByte();
                while (len != -1) {
                    if (len > 0) {
                        var buffer = ArrayPool<byte>.Shared.Rent(len*2);
                        try {
                            var memory = new Memory<byte>(buffer, 0, len*2);
                            await input.ReadAsync(memory, ct);
                            var tag = Encoding.Unicode.GetString(memory.Span);
                            snapshot.Add(tag);
                        }
                        finally {
                            ArrayPool<byte>.Shared.Return(buffer);
                        }
                    }
                    else {
                        snapshot.Add("");
                    }

                    len = input.ReadByte();
                }

                return new Result<object>(snapshot);
            }
            catch (Exception e) {
                return Result<object>.SerdeError(e.Message);
            }
        }
        
        public static async ValueTask UploadSetRecords(ILogger logger, IDataEngine de, int max,
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
            var arr = new byte[CalcSize(record)];
            SerializeRecord(record, arr);
            var evt = new Event(incId, arr);

            var recordId = GetRecordId(setName);
            outputs.Add(recordId);
            var result = await de.AppendEvent(recordId, evt, GetOffsetId(setName), ct);
                
            return result.IsOk
                ? new Result<string>(result.Value >= max ? setName : "") 
                : new Result<string>(result.Error); 
        }
        
        public static async Task<Result<Void>> UpdateSnapshot(IDataEngine de, ISerializer sysSer, IncrementId incId, 
            string setName, IIncrementArgsCollector argsCollector, List<EntityId> outputs, CancellationToken ct) {
            var snapshotId = GetSnapshotId(setName);
            var snapshotResult = await de.Retrieve(snapshotId, sysSer, ct);
            if (snapshotResult.IsError) 
                return new Result<Void>(snapshotResult.Error);
            
            if (snapshotResult.Value.IsOk) argsCollector.RegisterEntity(snapshotResult.Value.Key, false);
            var snapshot = snapshotResult.Value.IsOk ? (SetSnapshot) snapshotResult.Value.Value : new SetSnapshot();

            var recordId = GetRecordId(setName);
            var offsetId = GetOffsetId(setName);

            var eventLogResult = await de.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<Void>(eventLogResult.Error);

            var eventLog = eventLogResult.Value;
            argsCollector.RegisterEvent(recordId, eventLog.First, eventLog.Last);
            var reader = new SetRecordsReader(eventLog.Data);
            UpdateSnapshot(snapshot, reader);

            var snapshotStamp = new EntityStamp(snapshotId.CreateStampKey(incId), snapshot);
            var uploadSnapshotResult = await de.Upload(snapshotStamp, sysSer, ct);
            if (uploadSnapshotResult.IsError) return new Result<Void>(uploadSnapshotResult.Error);

            var truncateResult = await de.TruncateEvents(recordId, offsetId.CreateStampKey(incId), eventLog.Last, ct);
            if (truncateResult.IsError) return new Result<Void>(truncateResult.Error);

            outputs.Add(snapshotId);
            outputs.Add(offsetId);

            return new Result<Void>(Void.Value);
        }

        public static async Task<Result<SetSnapshot>> RetrieveSnapshot(IDataSource ds, ISerializer sysSer, 
            string setName, IIncrementArgsCollector argsCollector, CancellationToken ct) {
            
            var snapshotId = GetSnapshotId(setName);
            var snapshotResult = await ds.Retrieve(snapshotId, sysSer, ct);
            if (snapshotResult.IsError) 
                return new Result<SetSnapshot>(snapshotResult.Error);

            if (snapshotResult.Value.IsOk) argsCollector.RegisterEntity(snapshotResult.Value.Key, false);
            var snapshot = snapshotResult.Value.IsOk ? (SetSnapshot) snapshotResult.Value.Value : new SetSnapshot();

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