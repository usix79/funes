using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public static class IncrementEngine {
        private readonly struct WorkItem<TModel,TMsg,TSideEffect> {
            public EntityStamp FactStamp { get; init; }
            public IncrementId? ParentId { get; init; }
            public int Attempt { get; init; }
            public long StartMilliseconds { get; init; }
            public Task<Result<LogicEngine<TModel,TMsg,TSideEffect>.LogicResult>> Task { get; init; }
        }

        public static async Task<Result<IncrementId>> Run<TModel,TMsg,TSideEffect>(
            IncrementEngineEnv<TModel,TMsg,TSideEffect> env, Entity fact, CancellationToken ct = default) {
            
            var rootIncId = IncrementId.None;
            LinkedList<WorkItem<TModel,TMsg,TSideEffect>> workItems = new ();
            SemaphoreSlim semaphore = new (0, 1);

            try {
                var factStamp = new EntityStamp(fact, IncrementId.NewFactId());
                var uploadFactResult = await env.DataEngine.Upload(factStamp, env.Serializer, ct, true);
                if (uploadFactResult.IsError) {
                    LogError(factStamp.IncId, "RunIncrement", "UploadFactFailed", uploadFactResult.Error);
                }
                
                RunLogic(factStamp, null, 1);
                
                while (workItems.First != null) {
                    await semaphore.WaitAsync(ct);
                    await CheckLogicResults(); 
                }

                return new Result<IncrementId>(rootIncId);
            }
            catch (Exception x) {
                env.Logger.LogCritical(x, "{Lib} {Obj} {Op} {Exn}", "Funes", "IncrementEngine", "Loop", x);
                return Result<IncrementId>.Exception(x);
            }
            
            void RunLogic(EntityStamp aFactStamp, IncrementId? parentId, int attempt) {
                if (attempt <= env.MaxAttempts) {
                    var item = new WorkItem<TModel,TMsg,TSideEffect> {
                        FactStamp = aFactStamp, 
                        ParentId = parentId, 
                        Attempt = attempt, 
                        StartMilliseconds = env.ElapsedMilliseconds, 
                        Task = env.LogicEngine.Run(aFactStamp.Entity, null!, ct)
                    };
                    workItems.AddLast(item);
                    ReleaseSemaphoreWhenDone(item.Task);
                }
                else {
                    LogError(aFactStamp.IncId, "RunLogic", "MaxAttempts");
                }
            }

            async void ReleaseSemaphoreWhenDone(Task task) {
                try { 
                    await task;
                } 
                finally {
                    try { if (semaphore.CurrentCount == 0) semaphore.Release(); } catch (SemaphoreFullException) { }
                }
            }

            async ValueTask CheckLogicResults() {
                var node = workItems.First;
                while (node != null) {
                    var nextNode = node.Next;
                    if (node.Value.Task.IsCompleted) {
                        workItems.Remove(node);
                        if (node.Value.Task.IsCompletedSuccessfully && node.Value.Task.Result.IsOk) {
                            await ProcessLogicResult(node.Value);                            
                        }
                        else {
                            var ex = node.Value.Task.Exception;
                            if (ex is null) LogError(node.Value.FactStamp.IncId, "CheckLogic", "LogicFailed", node.Value.Task.Result.Error);
                            else LogError(node.Value.FactStamp.IncId, "CheckLogic", "LogicException", null, ex);
                            
                            RunLogic(node.Value.FactStamp, node.Value.ParentId, node.Value.Attempt + 1);
                        }
                    }
                    node = nextNode;
                }
            }

            async ValueTask ProcessLogicResult(WorkItem<TModel,TMsg,TSideEffect> item) {
                var result = item.Task.Result.Value;
                var parentId = item.ParentId;
                var incrementResult = await TryIncrement(item);

                if (incrementResult.IsOk) {
                    var (increment, derivedFacts) = incrementResult.Value;
                    
                    if (item.ParentId is null) rootIncId = increment.Id;
                    
                    await PerformSideEffects(increment.Id, result.SideEffects);

                    if (derivedFacts != null) {
                        foreach (var derivedFact in derivedFacts) {
                            RunLogic(derivedFact, increment.Id, 1);
                        }
                    }
                }
                else {
                    if (incrementResult.Error is Error.IncrementError x) {
                        LogError(x.Increment.Id, "ProcessLogicResult", "TryReflect", x.Error);

                        if (x.Error is Error.CommitError) {
                            // new attempt if transaction failed
                            if (parentId is null) rootIncId = x.Increment.Id;
                            parentId = x.Increment.Id;
                            RunLogic(item.FactStamp, parentId, item.Attempt + 1);
                        }
                    }
                    else {
                        LogError(item.FactStamp.IncId, "ProcessLogicResult", "TryReflect", incrementResult.Error);
                    }
                }
            }

            async ValueTask PerformSideEffects(IncrementId incId, List<TSideEffect> sideEffects) {
                var behaviorTasks = ArrayPool<Task>.Shared.Rent(sideEffects.Count);
                try {
                    for (var i = 0; i < sideEffects.Count; i++)
                        behaviorTasks[i] = env.Behavior(sideEffects[i], ct);
                    
                    // fill rest of the array with Task.CompletedTask
                    for (var i = sideEffects.Count; i < behaviorTasks.Length; i++)
                        behaviorTasks[i] = Task.CompletedTask;

                    await Task.WhenAll(behaviorTasks);
                }
                catch (AggregateException x) {
                    LogError(incId, "ProcessLogicResult", "SideEffect", null, x);
                }
                finally {
                    ArrayPool<Task>.Shared.Return(behaviorTasks);
                }
            }

            async ValueTask<Result<(Increment, List<EntityStamp>?)>> TryIncrement(
                WorkItem<TModel,TMsg,TSideEffect> item) {

                var logicResult = item.Task.Result.Value;
                try {
                    var builder = new IncrementBuilder(IncrementId.NewId(), item.ParentId, item.FactStamp.Key,
                        item.StartMilliseconds, item.Attempt, env.ElapsedMilliseconds);
                    
                    var commitResult = await TryCommit(builder.IncId, logicResult.Inputs, logicResult.Outputs); 
                    builder.RegisterCommitResult(commitResult, env.ElapsedMilliseconds);

                    List<EntityStamp>? derivedFacts = logicResult.DerivedFacts.Count > 0 ? new() : null;
                    foreach (var derivedFact in logicResult.DerivedFacts)
                        derivedFacts!.Add(derivedFact.ToStamp(builder.IncId));
                    
                    if (commitResult.IsOk) {
                        foreach (var outputEntity in logicResult.Outputs.Values) {
                            var outputStamp = outputEntity.ToStamp(builder.IncId);
                            builder.RegisterResult(await env.DataEngine.Upload(outputStamp, env.Serializer, ct));
                        }
                        
                        // TODO: upload indexes

                        if (derivedFacts != null) {
                            foreach (var derivedFact in derivedFacts) {
                                builder.RegisterResult(await env.DataEngine.Upload(derivedFact, env.Serializer, ct, true));
                            }
                        }
                    }

                    if (logicResult.SideEffects.Count > 0)
                        builder.RegisterSideEffects(DescribeSideEffects(logicResult.SideEffects));
                    
                    var increment = builder.Create(logicResult.Inputs, logicResult.Outputs, 
                        derivedFacts, logicResult.Constants, env.ElapsedMilliseconds); 
                    
                    var incrementStamp = Increment.CreateEntStamp(increment);
                    builder.RegisterResult(await env.DataEngine.Upload(incrementStamp, env.SystemSerializer, ct, true));

                    if (item.ParentId.HasValue) {
                        var childStamp = Increment.CreateChildEntStamp(increment.Id, item.ParentId.Value);
                        builder.RegisterResult(await env.DataEngine.Upload(childStamp, env.SystemSerializer, ct, true));
                    }

                    var error = builder.GetError();
                    if (error != Error.No) {
                        return Result<(Increment, List<EntityStamp>?)>.IncrementError(increment, error);
                    }
                    
                    return new Result<(Increment, List<EntityStamp>?)>((increment, derivedFacts));
                }
                catch (TaskCanceledException) { throw; }
                catch (Exception e) { return Result<(Increment, List<EntityStamp>?)>.Exception(e); }
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
                        if (isPremise) {
                            premisesArr[idx++] = entity.Id.CreateStampKey(entityIncId);
                        }
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
            
            string DescribeSideEffects(List<TSideEffect> sideEffects) {
                var txt = new StringBuilder();
                foreach (var effect in sideEffects)
                    if (effect != null) txt.AppendLine(effect.ToString());
                return txt.ToString();
            }

            void LogError(IncrementId incId, string op, string kind, Error? err = null, Exception? exn = null) {
                var msg = "{Lib} {Obj}, {Op} {Kind} {IncId} {Err}";
                if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
                if (exn is null) env.Logger.LogError(msg, "Funes", "IncrementEngine", op, kind, incId, err);
                else env.Logger.LogError(exn, msg, "Funes", "IncrementEngine",  op, kind, incId, err);
            }
        }

        private struct IncrementBuilder {
            private readonly Dictionary<string, string> _detailsDict;
            private readonly DateTimeOffset _reflectTime;
            readonly long _startCommitMilliseconds;
            private long _endCommitMilliseconds;
            IncrementStatus _status;
            List<Error>? _errors;
            private IncrementId _incId;
            private readonly IncrementId? _parentId;
            private readonly EntityStampKey _factKey;
            private readonly int _attempt;
            private readonly long _startMilliseconds;

            public IncrementId IncId => _incId;

            public IncrementBuilder(IncrementId incId, IncrementId? parentId, EntityStampKey factKey, 
                long startMilliseconds, int attempt, long ms) {
                _incId = incId;
                _parentId = parentId;
                _factKey = factKey;
                _startMilliseconds = startMilliseconds;
                _attempt = attempt;
                _startCommitMilliseconds = ms;
                _endCommitMilliseconds = 0;
                _detailsDict = new Dictionary<string, string>(8);
                _reflectTime = DateTimeOffset.UtcNow;
                _status = IncrementStatus.Unknown;
                _errors = null;
            }

            public void RegisterCommitResult(Result<Void> result, long ms) {
                _endCommitMilliseconds = ms;

                if (result.IsOk) {
                    _status = IncrementStatus.Success;
                }
                else {
                    if (result.Error is Error.CommitError err) {
                        _status = IncrementStatus.Fail;
                        _incId = IncId.AsFail();
                        _detailsDict[Increment.DetailsCommitErrors] =
                            string.Join(' ', err.Conflicts.Select(x => x.ToString()));
                    }
                    else {
                        _status = IncrementStatus.Lost;
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

            public void RegisterSideEffects(string txt) =>
                _detailsDict[Increment.DetailsSideEffects] = txt;
            
            public Error GetError() {
                if (_errors is null) return Error.No;
                if (_errors!.Count == 1) return _errors[0];
                return new Error.AggregateError(_errors!);
            }

            public Increment Create(Dictionary<EntityId, (Entity,IncrementId,bool)> inputs, Dictionary<EntityId, Entity> outputs, 
                List<EntityStamp>? derivedFacts, List<KeyValuePair<string,string>> constants, long ms) {

                var inputsArr = new KeyValuePair<EntityStampKey, bool>[inputs.Count];
                var idx = 0;
                foreach (var pair in inputs) {
                    inputsArr[idx++] = new KeyValuePair<EntityStampKey, bool>(
                        new EntityStampKey(pair.Key, pair.Value.Item2), pair.Value.Item3);
                }

                var derivedFactsArr = derivedFacts != null 
                    ? new EntityId[derivedFacts.Count] 
                    : Array.Empty<EntityId>();
                
                if (derivedFacts != null) {
                    idx = 0;
                    foreach (var derivedFact in derivedFacts)
                        derivedFactsArr[idx++] = derivedFact.EntId;
                }

                _detailsDict[Increment.DetailsIncrementTime] = _reflectTime.ToString();
                _detailsDict[Increment.DetailsAttempt] = _attempt.ToString();
                _detailsDict[Increment.DetailsLogicDuration] = (_startCommitMilliseconds - _startMilliseconds).ToString()!;
                _detailsDict[Increment.DetailsCommitDuration] = (_endCommitMilliseconds - _startCommitMilliseconds).ToString()!;
                _detailsDict[Increment.DetailsUploadDuration] = (ms - _endCommitMilliseconds).ToString()!;

                return new Increment(_incId, _parentId ?? IncrementId.None, _status, _factKey,
                    inputsArr, outputs .Keys.ToArray(), derivedFactsArr, constants, _detailsDict);
            }
        }
    }
}