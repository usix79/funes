using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Funes.Indexes;
using Funes.Sets;

namespace Funes {
    public static class IncrementEngine<TModel,TMsg,TSideEffect> {
        
        public static async Task<Result<IncrementId>> Run(
            IncrementEngineEnv<TModel,TMsg,TSideEffect> env, EntityEntry fact, CancellationToken ct = default) {
            
            try {
                for(var attempt = 1; attempt <= env.MaxAttempts; attempt++){
                    var start = env.ElapsedMilliseconds;
                    var args = new IncrementArgs();
                    var logicResult = await LogicEngine<TModel,TMsg,TSideEffect>
                        .Run(env.LogicEngineEnv, fact.Entity, null!, args, ct);

                    if (logicResult.IsOk) {
                        var incrementResult = await TryIncrement(logicResult.Value, args, attempt, start);
                        
                        switch (incrementResult.Error) {
                            case Error.NoError:
                                await PerformSideEffects(incrementResult.Value.Id, logicResult.Value.SideEffects);
                                return new Result<IncrementId>(incrementResult.Value.Id);
                            case Error.IncrementError {Error: Error.CommitError} x:
                                // new attempt if transaction failed
                                env.Logger.FunesErrorWarning("IncrementEngine", "Commit", x.Increment.Id, x);    
                                continue;
                            case Error.IncrementError x:
                                LogError(x.Increment.Id, "TryIncrement", incrementResult.Error);
                                return new Result<IncrementId>(x);
                            default:
                                LogError(fact.IncId, "TryIncrementGeneral", incrementResult.Error);
                                return new Result<IncrementId>(incrementResult.Error);
                        }
                    }
                    LogError(fact.IncId, "Logic", logicResult.Error);
                    return new Result<IncrementId>(logicResult.Error);
                }
                LogError(fact.IncId, "MaxAttempts");
                return new Result<IncrementId>(Error.MaxAttempts);
            }
            catch (Exception x) {
                LogException(fact.IncId,  "General", x);
                return Result<IncrementId>.Exception(x);
            }
            
            async ValueTask PerformSideEffects(IncrementId incId, List<TSideEffect> sideEffects) {
                if (sideEffects.Count == 0) return;
                
                var behaviorTasksArr = ArrayPool<ValueTask<Result<Void>>>.Shared.Rent(sideEffects.Count);
                var behaviorTasks = new ArraySegment<ValueTask<Result<Void>>>(behaviorTasksArr, 0, sideEffects.Count);
                var resultsArr = ArrayPool<Result<Void>>.Shared.Rent(sideEffects.Count);
                var results = new ArraySegment<Result<Void>>(resultsArr, 0, sideEffects.Count);
                try {
                    for (var i = 0; i < sideEffects.Count; i++)
                        behaviorTasks[i] = env.Behavior(incId, sideEffects[i], ct);
                    
                    await Utils.Tasks.WhenAll(behaviorTasks, results, ct);

                    foreach (var result in results)
                        if (result.IsError) LogError(incId, "SideEffect", result.Error);
                }
                finally {
                    ArrayPool<ValueTask<Result<Void>>>.Shared.Return(behaviorTasksArr);
                    ArrayPool<Result<Void>>.Shared.Return(resultsArr);
                }
            }

            async ValueTask<Result<Increment>> TryIncrement(
                LogicResult<TSideEffect> lgResult, IncrementArgs args, int attempt, long start) {

                var builder = new IncrementBuilder(fact.Key, start, attempt, env.ElapsedMilliseconds);
                
                var commitResult = await TryCommit(builder.IncId, args, lgResult.Entities); 
                builder.RegisterCommitResult(commitResult, env.ElapsedMilliseconds);

                var outputs = new List<EntityId>(lgResult.Entities.Keys);
                if (commitResult.IsOk) {
                    if (lgResult.Entities.Count > 0) {
                        var uploadResultsArr = ArrayPool<Result<Void>>.Shared.Rent(lgResult.Entities.Count);
                        try {
                            var uploadResults = new ArraySegment<Result<Void>>(uploadResultsArr, 0, lgResult.Entities.Count);
                            await UploadOutputs(builder.IncId, lgResult.Entities, uploadResults);
                            builder.RegisterResults(uploadResults);
                        }
                        finally {
                            ArrayPool<Result<Void>>.Shared.Return(uploadResultsArr);
                        }
                    }
                    
                    // TODO: DRY, consider abstract records processing for sets and indexes
                    if (lgResult.SetRecords.Count > 0) {
                        var uploadResultsArr = ArrayPool<Result<string>>.Shared.Rent(lgResult.SetRecords.Count);
                        var uploadResults = new ArraySegment<Result<string>>(uploadResultsArr, 0, lgResult.SetRecords.Count);
                        try {
                            await SetsModule.UploadSetRecords(env.DataEngine, env.MaxEventLogSize, 
                                builder.IncId, lgResult.SetRecords, outputs, uploadResults, ct);
                            
                            builder.RegisterResults(uploadResults);

                            foreach (var res in uploadResults) {
                                if (res.IsOk && !string.IsNullOrEmpty(res.Value)) {
                                    // TODO: consider updating snapshots in parallel
                                    var snapshotResult = await SetsModule.UpdateSnapshot(
                                        env.DataEngine, builder.IncId, res.Value, args, outputs, ct);
                                    builder.RegisterResult(snapshotResult);
                                }
                            }
                        }
                        finally {
                            ArrayPool<Result<string>>.Shared.Return(uploadResultsArr);
                        }
                    }

                    if (lgResult.IndexRecords.Count > 0) {
                        var uploadResultsArr = ArrayPool<Result<string>>.Shared.Rent(lgResult.IndexRecords.Count);
                        var uploadResults = new ArraySegment<Result<string>>(uploadResultsArr, 0, lgResult.IndexRecords.Count);
                        try {
                            await IndexesModule.UploadRecords(env.DataEngine, env.MaxEventLogSize, 
                                builder.IncId, lgResult.IndexRecords, outputs, uploadResults, ct);
                            
                            builder.RegisterResults(uploadResults);

                            foreach (var res in uploadResults) {
                                if (res.IsOk && !string.IsNullOrEmpty(res.Value)) {
                                    // TODO: rebuild index
                                }
                            }
                        }
                        finally {
                            ArrayPool<Result<string>>.Shared.Return(uploadResultsArr);
                        }
                    }
                }

                if (lgResult.SideEffects.Count > 0)
                    builder.DescribeSideEffects(DescribeSideEffects(lgResult.SideEffects));
                
                var increment = builder.Create(args, outputs, lgResult.Constants, env.ElapsedMilliseconds);

                builder.RegisterResult(await Increment.Upload(env.DataEngine, increment, ct));
                builder.RegisterResult(await Increment.UploadChild(env.DataEngine, increment.Id, fact.IncId, ct));
                
                var error = builder.GetError();
                return error == Error.No
                    ? new Result<Increment>(increment)
                    : Result<Increment>.IncrementError(increment, error);
            }

            async ValueTask<Result<Void>> TryCommit(IncrementId incId, 
                IncrementArgs args,
                Dictionary<EntityId, Entity> outputs) {

                var premisesCount = args.PremisesCount();

                var premisesArr = ArrayPool<StampKey>.Shared.Rent(premisesCount);
                var premises = new ArraySegment<StampKey>(premisesArr, 0, premisesCount);
                var conclusionsArr = ArrayPool<EntityId>.Shared.Rent(outputs.Count);
                var conclusions = new ArraySegment<EntityId>(conclusionsArr, 0, outputs.Count);
                try {
                    var idx = 0;
                    foreach (var link in args.Entities)
                        if (link.IsPremise) premises[idx++] = link.Key;
                    
                    outputs.Keys.CopyTo(conclusionsArr, 0);
                    
                    return await env.DataEngine.TryCommit(premises, conclusions, incId, ct);
                }
                finally {
                    ArrayPool<StampKey>.Shared.Return(premisesArr);
                    ArrayPool<EntityId>.Shared.Return(conclusionsArr);
                }
            }
            
            async ValueTask UploadOutputs(
                IncrementId incId, Dictionary<EntityId,Entity> outputs, ArraySegment<Result<Void>> results) {
                
                var uploadTasksArr = ArrayPool<ValueTask<Result<Void>>>.Shared.Rent(outputs.Count);
                var uploadTasks = new ArraySegment<ValueTask<Result<Void>>>(uploadTasksArr, 0, outputs.Count);
                try {
                    var idx = 0;
                    foreach (var outputEntity in outputs.Values)
                        uploadTasks[idx++] = SerializeAndUpload(outputEntity, incId); 

                    await Utils.Tasks.WhenAll(uploadTasks, results, ct);
                }
                finally {
                    ArrayPool<ValueTask<Result<Void>>>.Shared.Return(uploadTasksArr);
                }
            }

            async ValueTask<Result<Void>> SerializeAndUpload(Entity entity, IncrementId incId) {
                var serdeResult = env.Serializer.Encode(entity.Id, entity.Value);
                if (serdeResult.IsError) return new Result<Void>(serdeResult.Error);
                var stamp = new BinaryStamp(new StampKey(entity.Id, incId), serdeResult.Value);
                
                return await env.DataEngine.Upload(stamp, ct);
            }

            static string DescribeSideEffects(List<TSideEffect> sideEffects) {
                var txt = new StringBuilder();
                foreach (var effect in sideEffects)
                    if (effect != null) txt.AppendLine(effect.ToString());
                return txt.ToString();
            }

            void LogError(IncrementId incId, string kind, Error? err = null) =>
                env.Logger.FunesError("IncrementEngine", kind, incId, err??Error.No);

            void LogException(IncrementId incId, string kind, Exception exn) =>
                env.Logger.FunesException("IncrementEngine", kind, incId, exn);
        }
    }
}