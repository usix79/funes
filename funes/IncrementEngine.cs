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
                    var context = new DataContext(env.DataEngine, env.Serializer);
                    var logicResult = await LogicEngine<TModel,TMsg,TSideEffect>
                        .Run(env.LogicEngineEnv, context, fact.Entity, null!, ct);

                    if (logicResult.IsOk) {
                        var incrementResult = await TryIncrement(context, logicResult.Value, attempt, start);
                        
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

            async ValueTask<Result<Increment>> TryIncrement(DataContext context,
                LogicResult<TSideEffect> lgResult, int attempt, long start) {

                var builder = new IncrementBuilder(fact.Key, start, attempt, env.ElapsedMilliseconds);
                
                var commitResult = await TryCommit(context, builder.IncId, lgResult.Entities); 
                builder.RegisterCommitResult(commitResult, env.ElapsedMilliseconds);

                if (commitResult.IsOk) {
                    var tasksArr = ArrayPool<ValueTask<Result<Void>>>.Shared.Rent(lgResult.TotalCount);
                    var resultsArr = ArrayPool<Result<Void>>.Shared.Rent(lgResult.TotalCount);
                    try {
                        var idx = 0;
                        foreach (var pair in lgResult.Entities)
                            tasksArr[idx++] = context.Upload(pair.Value, builder.IncId, ct);
                        foreach (var pair in lgResult.SetRecords)
                            tasksArr[idx++] = ProcessSetRecord(context, builder.IncId, pair.Key, pair.Value);
                        foreach (var pair in lgResult.IndexRecords)
                            tasksArr[idx++] = ProcessIndexRecord(context, builder.IncId, pair.Key, pair.Value);
                        
                        var tasks = new ArraySegment<ValueTask<Result<Void>>>(tasksArr, 0, idx);
                        var results = new ArraySegment<Result<Void>>(resultsArr, 0, idx);
                        await Utils.Tasks.WhenAll(tasks, results, ct);
                        foreach(var result in results)
                            builder.RegisterResult(result);
                    }
                    finally {
                        ArrayPool<ValueTask<Result<Void>>>.Shared.Return(tasksArr);
                        ArrayPool<Result<Void>>.Shared.Return(resultsArr);
                    }
                }

                if (lgResult.SideEffects.Count > 0)
                    builder.DescribeSideEffects(DescribeSideEffects(lgResult.SideEffects));
                
                var increment = builder.Create(context, lgResult.Constants, env.ElapsedMilliseconds);

                builder.RegisterResult(await Increment.Upload(env.DataEngine, increment, ct));
                builder.RegisterResult(await Increment.UploadChild(env.DataEngine, increment.Id, fact.IncId, ct));
                
                var error = builder.GetError();
                return error == Error.No
                    ? new Result<Increment>(increment)
                    : Result<Increment>.IncrementError(increment, error);
            }

            async ValueTask<Result<Void>> TryCommit(
                DataContext context, IncrementId incId, Dictionary<EntityId, Entity> outputs) {

                var premisesCount = context.PremisesCount();

                var premisesArr = ArrayPool<StampKey>.Shared.Rent(premisesCount);
                var premises = new ArraySegment<StampKey>(premisesArr, 0, premisesCount);
                var conclusionsArr = ArrayPool<EntityId>.Shared.Rent(outputs.Count);
                var conclusions = new ArraySegment<EntityId>(conclusionsArr, 0, outputs.Count);
                try {
                    var idx = 0;
                    foreach (var inputData in context.InputEntities.Values)
                        if (inputData.IsRealPremise) premises[idx++] = inputData.StampResult.Value.Key;
                    
                    outputs.Keys.CopyTo(conclusionsArr, 0);
                    
                    return await env.DataEngine.TryCommit(premises, conclusions, incId, ct);
                }
                finally {
                    ArrayPool<StampKey>.Shared.Return(premisesArr);
                    ArrayPool<EntityId>.Shared.Return(conclusionsArr);
                }
            }

            async ValueTask<Result<Void>> ProcessSetRecord(DataContext context, IncrementId incId,
                string setName, SetRecord record) {
                var uploadResult = await SetsModule.UploadSetRecord(context, ct, incId, setName, record);

                if (uploadResult.IsError) return new Result<Void>(uploadResult.Error);

                return uploadResult.Value >= env.MaxEventLogSize
                    ? await SetsModule.UpdateSnapshot(context, ct, incId, setName)
                    : new Result<Void>(Void.Value);
            }
            
            async ValueTask<Result<Void>> ProcessIndexRecord(DataContext context, IncrementId incId,
                string indexName, IndexRecord record) {
                var uploadResult = await IndexesModule.UploadRecord(context, ct, incId, indexName, record);

                if (uploadResult.IsError) return new Result<Void>(uploadResult.Error);

                return uploadResult.Value >= env.MaxEventLogSize
                    ? await IndexesModule.UpdateIndex(env.Logger, context, ct, incId, indexName, env.MaxEventLogSize)
                    : new Result<Void>(Void.Value);
            }
            
            void LogError(IncrementId incId, string kind, Error? err = null) =>
                env.Logger.FunesError("IncrementEngine", kind, incId, err??Error.No);

            void LogException(IncrementId incId, string kind, Exception exn) =>
                env.Logger.FunesException("IncrementEngine", kind, incId, exn);
        }
        static string DescribeSideEffects(List<TSideEffect> sideEffects) {
            var txt = new StringBuilder();
            foreach (var effect in sideEffects)
                if (effect != null) txt.AppendLine(effect.ToString());
            return txt.ToString();
        }
    }
}