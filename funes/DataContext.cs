using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes {
    public class DataContext : IDataSource {
        
        private readonly IDataEngine _dataEngine;
        private readonly ISerializer _serializer;
        
        public readonly struct InputEntity {
            public Result<BinaryStamp> StampResult { get; }
            public EntityEntry? Entry { get; }
            public bool AsPremise { get; }
            public InputEntity(Result<BinaryStamp> stampResult, EntityEntry? entryResult,  bool isPremise) => 
                (StampResult, Entry, AsPremise) = (stampResult, entryResult, isPremise);

            public bool IsRealPremise => AsPremise && StampResult.IsOk && !StampResult.Value.IsEmpty;
        }
        
        private readonly ConcurrentDictionary<EntityId, InputEntity> _inputEntities = new ();
        private readonly ConcurrentDictionary<EntityId, Result<EventLog>> _inputEventLogs = new ();
        private readonly ConcurrentDictionary<string, Result<IReadOnlySet<string>>> _sets = new ();
        private readonly ConcurrentBag<EntityId> _outputs = new();

        public DataContext(IDataEngine dataEngine, ISerializer serializer) => 
            (_dataEngine, _serializer) = (dataEngine, serializer);

        private EntityEntry CreateEntry(EntityId eid, Result<BinaryStamp> stampResult) {
            var entry = EntityEntry.NotAvailable(eid);
            if (stampResult.IsOk) {
                var stamp = stampResult.Value;
                if (stamp.IsEmpty) {
                    entry = EntityEntry.NotExist(eid);
                }
                else {
                    var serdeResult = _serializer.Decode(eid, stamp.Data);
                    entry = serdeResult.IsOk
                        ? EntityEntry.Ok(new Entity(eid, serdeResult.Value), stamp.IncId)
                        : EntityEntry.NotAvailable(eid);
                }
            }

            return entry;
        }

        public bool TryGetEntity(EntityId eid, out Result<BinaryStamp> stampResult) {
            if (!_inputEntities.TryGetValue(eid, out var inputEntity)) {
                stampResult = new Result<BinaryStamp>(Error.NotFound);
                return false;
            }

            stampResult = inputEntity.StampResult;
            return true;
        }

        public bool TryGetEntry(EntityId eid, bool asPremise, out EntityEntry entry) {
            if (!_inputEntities.TryGetValue(eid, out var inputEntity)) {
                entry = default;
                return false;
            }

            entry = inputEntity.Entry ?? CreateEntry(eid, inputEntity.StampResult);

            if (!inputEntity.Entry.HasValue || (asPremise && !inputEntity.AsPremise)) {
                _inputEntities[eid] = new InputEntity(
                    inputEntity.StampResult, entry, inputEntity.AsPremise || asPremise);    
            }
            
            return true;
        }

        private static readonly HashSet<string> EmptySet = new ();
        
        public Result<IReadOnlySet<string>> GetSet(string setName) {
            if (_sets.TryGetValue(setName, out var setResult))
                return setResult;

            return Result<IReadOnlySet<string>>.NotFound;
        }

        public async ValueTask<Result<BinaryStamp>> Retrieve(EntityId eid, CancellationToken ct) {
            if (_inputEntities.TryGetValue(eid, out var inputEntity))
                return inputEntity.StampResult;

            var retrieveResult = await _dataEngine.Retrieve(eid, ct);
            _inputEntities[eid] = new InputEntity(retrieveResult, null, false);
            return retrieveResult;
        }

        public async ValueTask<Result<EventLog>> RetrieveEventLog(
            EntityId recordsId, EntityId offsetId, CancellationToken ct) {
            
            if (_inputEventLogs.TryGetValue(recordsId, out var result)) return result;
            result = await _dataEngine.RetrieveEventLog(recordsId, offsetId, ct);
            _inputEventLogs[recordsId] = result;
            return result;
        }

        public async ValueTask<Result<IReadOnlySet<string>>> RetrieveSetSnapshot(string setName, CancellationToken ct) {
            var retrieveResult = await SetsModule.RetrieveSnapshot(this, setName, ct);
            var setResult = retrieveResult.IsOk
                ? new Result<IReadOnlySet<string>>(retrieveResult.Value.CreateSet())
                : retrieveResult.Error == Error.NotFound
                    ? new Result<IReadOnlySet<string>>(EmptySet)
                    : new Result<IReadOnlySet<string>>(retrieveResult.Error);
            
            _sets[setName] = setResult;
            return setResult;
        }

        public int PremisesCount() {
            var count = 0;
            foreach (var pair in _inputEntities) {
                if (pair.Value.AsPremise) count++;
            }

            return count;
        }

        public IReadOnlyDictionary<EntityId, InputEntity> InputEntities => _inputEntities;

        public List<Increment.InputEntity> GetInputList() {
            var result = new List<Increment.InputEntity>(_inputEntities.Count);
            foreach (var input in _inputEntities) {
                if (input.Value.StampResult.IsOk && input.Value.StampResult.Value.IsNotEmpty)
                    result.Add(new Increment.InputEntity(input.Value.StampResult.Value.Key, input.Value.AsPremise));
            }

            return result;
        }

        public List<Increment.InputEventLog> GetEventLogInputList() {
            var result = new List<Increment.InputEventLog>(_inputEventLogs.Count);
            foreach (var input in _inputEventLogs) {
                if (input.Value.IsOk)
                    result.Add(new Increment.InputEventLog
                        {Id = input.Key, FirstIncId = input.Value.Value.First, LastIncId = input.Value.Value.Last});
            }
            return result;
        }

        public List<EntityId> GetOutputs() {
            return _outputs.ToList();
        }
        
        public ValueTask<Result<Void>> Upload(
            Entity entity, IncrementId incId, CancellationToken ct, bool skipCache = false) {
            var serdeResult = _serializer.Encode(entity.Id, entity.Value);
            if (serdeResult.IsError) return ValueTask.FromResult(new Result<Void>(serdeResult.Error));
            var stamp = new BinaryStamp(new StampKey(entity.Id, incId), serdeResult.Value);
            return UploadBinary(stamp, ct, skipCache);
        }

        public ValueTask<Result<Void>> UploadBinary(BinaryStamp stamp, CancellationToken ct, bool skipCache = false) {
            _outputs.Add(stamp.Eid);
            return _dataEngine.Upload(stamp, ct,skipCache);
        }

        public ValueTask<Result<int>> AppendEvent(EntityId recordId, Event evt, EntityId offsetId, CancellationToken ct) {
            _outputs.Add(recordId);
            return _dataEngine.AppendEvent(recordId, evt, offsetId, ct);
        }

        public ValueTask<Result<Void>> TryCommit(StampKey[] premises, EntityId[] conclusions, IncrementId incId, CancellationToken ct) {
            return _dataEngine.TryCommit(premises, conclusions, incId, ct);
        }

        public ValueTask<Result<Void>> TruncateEvents(EntityId recordId, IncrementId eventLogLast, CancellationToken ct) {
            return _dataEngine.TruncateEvents(recordId, eventLogLast, ct);
        }
    }
}