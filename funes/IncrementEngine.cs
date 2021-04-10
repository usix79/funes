using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public static class IncrementEngine<TModel,TMsg,TSideEffect> {
        
        public static async Task<Result<IncrementId>> Run(
            IncrementEngineEnv<TModel,TMsg,TSideEffect> env, EntityStamp factStamp, CancellationToken ct = default) {
            
            try {
                for(var attempt = 1; attempt <= env.MaxAttempts; attempt++){
                    var start = env.ElapsedMilliseconds; 
                    var logicResult = await env.LogicEngine.Run(factStamp.Entity, null!, ct);

                    if (logicResult.IsOk) {
                        var incrementResult = await TryIncrement(logicResult.Value, attempt, start);
                        
                        switch (incrementResult.Error) {
                            case Error.NoError:
                                await PerformSideEffects(incrementResult.Value.Id, logicResult.Value.SideEffects);
                                return new Result<IncrementId>(incrementResult.Value.Id);
                            case Error.IncrementError {Error: Error.CommitError} x:
                                // new attempt if transaction failed
                                env.Logger.LogWarning(LogTemplate, 
                                    "Funes", "IncrementEngine", "Commit", x.Increment.Id.Id, x);    
                                continue;
                            case Error.IncrementError x:
                                LogError(x.Increment.Id, "TryIncrement", incrementResult.Error);
                                return new Result<IncrementId>(incrementResult.Error);
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
                LogError(factStamp.IncId,  "General", null, x);
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
                    LogError(incId, "PerformSideEffects", null, x);
                }
                finally {
                    ArrayPool<Task>.Shared.Return(behaviorTasks);
                }
            }

            async ValueTask<Result<Increment>> TryIncrement(
                LogicEngine<TModel,TMsg,TSideEffect>.LogicResult logicResult, int attempt, long start) {

                var builder = new IncrementBuilder(factStamp.Key, start, attempt, env.ElapsedMilliseconds);
                
                var commitResult = await TryCommit(builder.IncId, logicResult.Inputs, logicResult.Outputs); 
                builder.RegisterCommitResult(commitResult, env.ElapsedMilliseconds);

                if (commitResult.IsOk) {
                    if (logicResult.Outputs.Count > 0) {
                        builder.RegisterResults(await UploadOutputs(builder.IncId, logicResult.Outputs));
                    }
                    
                    // TODO: upload indexes
                }

                if (logicResult.SideEffects.Count > 0)
                    builder.DescribeSideEffects(DescribeSideEffects(logicResult.SideEffects));
                
                var increment = builder.Create(logicResult.Inputs, 
                    logicResult.Outputs, logicResult.Constants, env.ElapsedMilliseconds); 
                
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
                Dictionary<EntityId, (Entity,IncrementId,bool)> inputs,
                Dictionary<EntityId, Entity> outputs) {

                var premisesCount = 0;
                foreach (var tuple in inputs.Values) {
                    if (tuple.Item3) premisesCount++;
                }

                var premisesArr = ArrayPool<EntityStampKey>.Shared.Rent(premisesCount);
                var outputsArr = ArrayPool<EntityId>.Shared.Rent(outputs.Count);
                try {
                    var idx = 0;
                    foreach (var (entity, entityIncId, isPremise) in inputs.Values) {
                        if (isPremise) premisesArr[idx++] = entity.Id.CreateStampKey(entityIncId);
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
                    LogError(incId, "Upload Outputs", null, x);
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
            
            void LogError(IncrementId incId, string kind, Error? err = null, Exception? exn = null) {
                if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
                if (exn is null) env.Logger.LogError(LogTemplate, "Funes", "IncrementEngine", kind, incId.Id, err);
                else env.Logger.LogError(exn, LogTemplate, "Funes", "IncrementEngine",  kind, incId.Id, err);
            }

        }

        private const string LogTemplate = "{Lib} {Obj}, {Kind} {IncId} {Err}";

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

            public void DescribeSideEffects(string txt) => AppendDetails(Increment.DetailsSideEffects, txt);
            
            public Error GetError() {
                if (_errors is null) return Error.No;
                if (_errors!.Count == 1) return _errors[0];
                return new Error.AggregateError(_errors!);
            }

            public Increment Create(Dictionary<EntityId, (Entity,IncrementId,bool)> inputs, 
                Dictionary<EntityId, Entity> outputs, List<KeyValuePair<string,string>> constants, long ms) {

                var inputsArr = new KeyValuePair<EntityStampKey, bool>[inputs.Count];
                var idx = 0;
                foreach (var pair in inputs) {
                    inputsArr[idx++] = new KeyValuePair<EntityStampKey, bool>(
                        new EntityStampKey(pair.Key, pair.Value.Item2), pair.Value.Item3);
                }
                
                AppendDetails(Increment.DetailsIncrementTime, _incrementTime.ToString());
                AppendDetails(Increment.DetailsAttempt, _attempt.ToString());
                AppendDetails(Increment.DetailsLogicDuration, (_startCommitMilliseconds - _startMilliseconds).ToString()!);
                AppendDetails(Increment.DetailsCommitDuration, (_endCommitMilliseconds - _startCommitMilliseconds).ToString()!);
                AppendDetails(Increment.DetailsUploadDuration, (ms - _endCommitMilliseconds).ToString()!);

                if (_errors != null) {
                    foreach (var error in _errors) AppendDetails(Increment.DetailsError, error.ToString());
                }

                return new Increment(_incId, _factKey, inputsArr, outputs .Keys.ToArray(), constants, _details);
            }

            public void AppendDetails(string key, string value) {
                _details.Add(new KeyValuePair<string, string>(key, value));
            }
        }
    }
}