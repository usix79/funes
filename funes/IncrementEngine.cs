using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Funes.Sets;

namespace Funes {
    public static class IncrementEngine<TModel,TMsg,TSideEffect> {
        
        public static async Task<Result<IncrementId>> Run(
            IncrementEngineEnv<TModel,TMsg,TSideEffect> env, EntityStamp factStamp, CancellationToken ct = default) {
            
            try {
                for(var attempt = 1; attempt <= env.MaxAttempts; attempt++){
                    var start = env.ElapsedMilliseconds;
                    var args = new IncrementArgs();
                    var logicResult = await env.LogicEngine.Run(factStamp.Entity, null!, args, ct);

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
                                LogError(factStamp.IncId, "TryIncrementGeneral", incrementResult.Error);
                                return new Result<IncrementId>(incrementResult.Error);
                        }
                    }
                    LogError(factStamp.IncId, "Logic", logicResult.Error);
                    return new Result<IncrementId>(logicResult.Error);
                }
                LogError(factStamp.IncId, "MaxAttempts");
                return new Result<IncrementId>(Error.MaxAttempts);
            }
            catch (Exception x) {
                LogException(factStamp.IncId,  "General", x);
                return Result<IncrementId>.Exception(x);
            }
            
            async ValueTask PerformSideEffects(IncrementId incId, List<TSideEffect> sideEffects) {
                if (sideEffects.Count == 0) return;
                
                var behaviorTasks = ArrayPool<Task>.Shared.Rent(sideEffects.Count);
                try {
                    for (var i = 0; i < sideEffects.Count; i++)
                        behaviorTasks[i] = env.Behavior(incId, sideEffects[i], ct);
                    
                    // fill rest of the array with Task.CompletedTask
                    for (var i = sideEffects.Count; i < behaviorTasks.Length; i++)
                        behaviorTasks[i] = Task.CompletedTask;

                    await Task.WhenAll(behaviorTasks);
                }
                catch (AggregateException x) {
                    LogException(incId, "PerformSideEffects", x);
                }
                finally {
                    ArrayPool<Task>.Shared.Return(behaviorTasks);
                }
            }

            async ValueTask<Result<Increment>> TryIncrement(
                LogicEngine<TModel,TMsg,TSideEffect>.LogicResult lg, IncrementArgs args, int attempt, long start) {

                var builder = new IncrementBuilder(factStamp.Key, start, attempt, env.ElapsedMilliseconds);
                
                var commitResult = await TryCommit(builder.IncId, args, lg.Entities); 
                builder.RegisterCommitResult(commitResult, env.ElapsedMilliseconds);

                var outputs = new List<EntityId>(lg.Entities.Keys);
                if (commitResult.IsOk) {
                    if (lg.Entities.Count > 0) {
                        builder.RegisterResults(await UploadOutputs(builder.IncId, lg.Entities));
                    }

                    if (lg.SetRecords.Count > 0) {
                        var setsResults = await SetsHelpers.UploadSetRecords(env.Logger, env.DataEngine, 
                            env.MaxEventLogSize, builder.IncId, lg.SetRecords, outputs, ct);
                        builder.RegisterResults(setsResults);

                        foreach (var res in setsResults) {
                            if (res.IsOk && !String.IsNullOrEmpty(res.Value)) {
                                // TODO: consider updating snapshots in parallel
                                var snapshotResult = await SetsHelpers.UpdateSnapshot(
                                    env.DataEngine, env.SystemSerializer, builder.IncId, res.Value, args, outputs, ct);
                                builder.RegisterResult(snapshotResult);
                            }
                        }
                    }
                    
                    // TODO: indexes
                }

                if (lg.SideEffects.Count > 0)
                    builder.DescribeSideEffects(DescribeSideEffects(lg.SideEffects));
                
                var increment = builder.Create(args, outputs, lg.Constants, env.ElapsedMilliseconds); 
                
                builder.RegisterResult(await env.DataEngine.Upload(
                    Increment.CreateStamp(increment), env.SystemSerializer, ct, true));
                builder.RegisterResult(await env.DataEngine.Upload(
                    Increment.CreateChildStamp(increment.Id, factStamp.IncId), env.SystemSerializer, ct, true));

                var error = builder.GetError();
                return error == Error.No
                    ? new Result<Increment>(increment)
                    : Result<Increment>.IncrementError(increment, error);
            }

            async ValueTask<Result<Void>> TryCommit(IncrementId incId, 
                IncrementArgs args,
                Dictionary<EntityId, Entity> outputs) {

                var premisesCount = args.PremisesCount();

                var premisesArr = ArrayPool<EntityStampKey>.Shared.Rent(premisesCount);
                var outputsArr = ArrayPool<EntityId>.Shared.Rent(outputs.Count);
                try {
                    var idx = 0;
                    foreach (var link in args.Entities) {
                        if (link.IsPremise) premisesArr[idx++] = link.Key;
                    }
                    var premises = new ArraySegment<EntityStampKey>(premisesArr, 0, premisesCount);
                    
                    outputs.Keys.CopyTo(outputsArr, 0);
                    var conclusions = new ArraySegment<EntityId>(outputsArr, 0, outputs.Count);
                    
                    return await env.DataEngine.TryCommit(premises, conclusions, incId, ct);
                }
                finally {
                    ArrayPool<EntityStampKey>.Shared.Return(premisesArr);
                    ArrayPool<EntityId>.Shared.Return(outputsArr);
                }
            }
            
            Task<Result<Void>[]> UploadOutputs(IncrementId incId, Dictionary<EntityId,Entity> outputs) {
                var uploadTasks = ArrayPool<Task<Result<Void>>>.Shared.Rent(outputs.Count);
                try {
                    var idx = 0;
                    foreach (var outputEntity in outputs.Values) {
                        var outputStamp = outputEntity.ToStamp(incId);
                        uploadTasks[idx++] = env.DataEngine.Upload(outputStamp, env.Serializer, ct).AsTask();
                    }

                    // fill rest of the array with Task.CompletedTask
                    for (var i = outputs.Count; i < uploadTasks.Length; i++)
                        uploadTasks[i] = Result<Void>.CompletedTask;

                    return Task.WhenAll(uploadTasks);
                }
                catch (AggregateException x) {
                    LogException(incId, "Upload Outputs", x);
                    return Task.FromResult(new []{Result<Void>.Exception(x)});
                }
                finally {
                    ArrayPool<Task<Result<Void>>>.Shared.Return(uploadTasks);
                }
            }


            string DescribeSideEffects(List<TSideEffect> sideEffects) {
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


        private struct IncrementBuilder {
            private readonly EntityStampKey _factKey;
            private readonly long _startMilliseconds;
            private readonly int _attempt;
            private readonly long _startCommitMilliseconds;
            private readonly DateTimeOffset _incrementTime;
            private IncrementId _incId;
            private readonly List<KeyValuePair<string,string>> _details;
            private long _endCommitMilliseconds;
            List<Error>? _errors;

            public IncrementId IncId => _incId;

            public IncrementBuilder(EntityStampKey factKey, long startMilliseconds, int attempt, long ms) {
                _incId = IncrementId.NewId();
                _factKey = factKey;
                _startMilliseconds = startMilliseconds;
                _attempt = attempt;
                _startCommitMilliseconds = ms;
                _endCommitMilliseconds = 0;
                _details = new List<KeyValuePair<string,string>>(8);
                _incrementTime = DateTimeOffset.UtcNow;
                _errors = null;
            }

            public void RegisterCommitResult(Result<Void> result, long ms) {
                _endCommitMilliseconds = ms;
                if (result.IsError) {
                    if (result.Error is Error.CommitError err) {
                        _incId = IncId.AsFail();
                        AppendDetails(Increment.DetailsCommitErrors, err.ToString());
                    }
                    else {
                        _incId = _incId.AsLost();
                    }
                    _errors ??= new List<Error>();
                    _errors.Add(result.Error);
                }
            }

            public void RegisterResult(Result<Void> result) {
                if (result.IsError) {
                    _errors ??= new List<Error>();
                    _errors.Add(result.Error);
                }
            }

            public void RegisterResults(Result<Void>[] results) {
                foreach (var result in results) {
                    if (result.IsError) {
                        _errors ??= new List<Error>();
                        _errors.Add(result.Error);
                    }
                }
            }

            public void RegisterResults(Result<string>[] results) {
                foreach (var result in results) {
                    if (result.IsError) {
                        _errors ??= new List<Error>();
                        _errors.Add(result.Error);
                    }
                }
            }

            public void DescribeSideEffects(string txt) => AppendDetails(Increment.DetailsSideEffects, txt);
            
            public Error GetError() {
                if (_errors is null) return Error.No;
                if (_errors!.Count == 1) return _errors[0];
                return new Error.AggregateError(_errors!);
            }

            public Increment Create(IncrementArgs args, List<EntityId> outputs, 
                List<KeyValuePair<string,string>> constants, long ms) {
                
                AppendDetails(Increment.DetailsIncrementTime, _incrementTime.ToString());
                AppendDetails(Increment.DetailsAttempt, _attempt.ToString());
                AppendDetails(Increment.DetailsLogicDuration, (_startCommitMilliseconds - _startMilliseconds).ToString()!);
                AppendDetails(Increment.DetailsCommitDuration, (_endCommitMilliseconds - _startCommitMilliseconds).ToString()!);
                AppendDetails(Increment.DetailsUploadDuration, (ms - _endCommitMilliseconds).ToString()!);

                if (_errors != null) {
                    foreach (var error in _errors) AppendDetails(Increment.DetailsError, error.ToString());
                }

                return new Increment(_incId, _factKey, args, outputs, constants, _details);
            }

            private void AppendDetails(string key, string value) {
                _details.Add(new KeyValuePair<string, string>(key, value));
            }
        }
    }
}