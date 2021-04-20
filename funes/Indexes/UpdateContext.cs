using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Funes.Impl;

namespace Funes.Indexes {
    
    public class UpdateContext {

        private enum InnerOpKind {Insert = 1, Remove = 2}
        
        private readonly struct InnerOp {
            public InnerOp(InnerOpKind kind, string key, string value) {
                Kind = kind;
                Key = key;
                Value = value;
            }
            public InnerOpKind Kind { get; }
            public string Key { get; }
            public string Value { get; }
        }

        private readonly IDataSource _ds;
        private readonly string _indexName;
        private List<InnerOp> _registeredOps = new();
        
        public Dictionary<EntityId, EntityEntry> RetrievedEntities { get; } = new();
        public Dictionary<EntityId, Entity> UpdatedEntities { get; } = new();

        public UpdateContext(IDataSource ds, string indexName) {
            _ds = ds;
            _indexName = indexName;
        }

        public async ValueTask<Result<Void>> RegisterOp(IndexOp op, CancellationToken ct) {
            
            switch (op.OpKind) {
                case IndexOp.Kind.Update:
                    var currentValueRes = await GetCurrentValue(op.Key, ct);
                    if (currentValueRes.IsError) return new Result<Void>(currentValueRes.Error);

                    if (currentValueRes.Value != "") {
                        var addRemoveRes = await AddRemoveOp(op.Key, currentValueRes.Value);
                        if (addRemoveRes.IsError) return new Result<Void>(addRemoveRes.Error);
                    }

                    var addInsertRes = await AddInsertOp(op.Key, op.Value);
                    if (addInsertRes.IsError) return new Result<Void>(addInsertRes.Error);
                    break;
                case IndexOp.Kind.Remove:
                    currentValueRes = await GetCurrentValue(op.Key, ct);
                    if (currentValueRes.IsError) return new Result<Void>(currentValueRes.Error);

                    if (currentValueRes.Value != "") {
                        var addRemoveRes = await AddRemoveOp(op.Key, currentValueRes.Value);
                        if (addRemoveRes.IsError) return new Result<Void>(addRemoveRes.Error);
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }

            return new Result<Void>(Void.Value);
        }

        public void Update() {
            
        }

        private ValueTask<Result<string>> GetCurrentValue(string key, CancellationToken ct) {
            // var keyId = IndexesHelpers.GetKeyId(_indexName, key);
            // if (!RetrievedEntities.TryGetValue(keyId, out var entry)) {
            //     var retrieveResult = await _ds.Retrieve(keyId, StringSerializer.Instance, ct);
            //     if (retrieveResult.IsError) return new Result<string>(retrieveResult.Error);
            //     RetrievedEntities[keyId] = entry = retrieveResult.Value;
            // }
            //
            // return new Result<string>(entry.IsOk ? (string)entry.Value : "");
            throw new NotImplementedException();
        }

        private ValueTask<Result<Void>> AddRemoveOp(string key, string value) {
            throw new NotImplementedException();
        }

        private ValueTask<Result<Void>> AddInsertOp(string key, string value) {
            throw new NotImplementedException();
        }

        
    }
}