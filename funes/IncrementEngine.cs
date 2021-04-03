using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class IncrementEngine<TModel,TMsg,TSideEffect> {
        private readonly int _maxAttempts;
        private readonly ILogger _logger;
        private readonly ISerializer _serializer;
        private readonly ISerializer _systemSerializer = new SystemSerializer();
        private readonly IDataEngine _dataEngine;
        private readonly LogicEngine<TModel, TMsg, TSideEffect> _logicEngine;
        private readonly Behavior<TSideEffect> _behavior;

        public IncrementEngine(
                LogicEngine<TModel, TMsg, TSideEffect> logicEngine, 
                Behavior<TSideEffect> behavior,
                ISerializer serializer,
                IDataEngine de, 
                ILogger logger, 
                int maxAttempts = 3) {
            (_logicEngine, _behavior, _serializer, _dataEngine, _logger, _maxAttempts) = 
                (logicEngine, behavior, serializer, de, logger, maxAttempts);
        }
        
        private readonly struct WorkItem {
            public Entity Fact { get; init; }
            public IncrementId? ParentId { get; init; }
            public int Attempt { get; init; }
            public long StartMilliseconds { get; init; }
            public Task<Result<LogicEngine<TModel,TMsg,TSideEffect>.LogicResult>> Task { get; init; }
        }

        public async Task<Result<IncrementId>> Run(Entity fact, CancellationToken ct = default) {
            
            var rootIncId = IncrementId.None;
            var stopWatch = Stopwatch.StartNew();
            
            LinkedList<WorkItem> workItems = new ();

            try {
                RunLogic(fact, null, 1);

                while (workItems.First != null) {
                    await Task.WhenAny(workItems.Select(holder => holder.Task));
                    ct.ThrowIfCancellationRequested();
                    await CheckLogicResults();
                }

                return new Result<IncrementId>(rootIncId);
            }
            catch (Exception x) {
                _logger.LogCritical(x, "{Lib} {Obj} {Op} {Exn}", "Funes", "IncrementEngine", "Loop", x);
                return Result<IncrementId>.Exception(x);
            }
            finally {
                stopWatch.Stop();
                await _dataEngine.Flush();
            }
            
            void RunLogic(Entity aFact, IncrementId? parentId, int attempt) {
                if (attempt <= _maxAttempts) {
                    var task = _logicEngine.Run(aFact, null!, ct);
                    var item = new WorkItem {
                        Fact = aFact, 
                        ParentId = parentId, 
                        Attempt = attempt, 
                        StartMilliseconds = stopWatch!.ElapsedMilliseconds, 
                        Task = task
                    };
                    workItems.AddLast(item);
                }
                else {
                    LogError(aFact, "RunLogic", "MaxAttempts");
                }
            }

            async ValueTask CheckLogicResults() {
                var node = workItems.First;
                while (node != null) {
                    if (node.Value.Task.IsCompleted) {
                        workItems.Remove(node);
                        if (node.Value.Task.IsCompletedSuccessfully && node.Value.Task.Result.IsOk) {
                            var item = node.Value;
                            await ProcessLogicResult(
                                item.Fact, item.ParentId, item.Attempt, item.StartMilliseconds, item.Task.Result.Value);                            
                        }
                        else {
                            var ex = node.Value.Task.Exception;
                            if (ex is null) LogError(node.Value.Fact, "CheckLogic", "LogicFailed", node.Value.Task.Result.Error);
                            else LogError(node.Value.Fact, "CheckLogic", "LogicException", null, ex);
                            
                            RunLogic(node.Value.Fact, node.Value.ParentId, node.Value.Attempt + 1);
                        }
                    }
                    node = node.Next;
                }
            }

            async ValueTask ProcessLogicResult(Entity aFact, IncrementId? parentId, int attempt, 
                long startMilliseconds, LogicEngine<TModel, TMsg, TSideEffect>.LogicResult result) {

                var cognitionResult = await TryIncrement(aFact, parentId, attempt, startMilliseconds, result);
                
                if (cognitionResult.IsOk) {
                    var incId = cognitionResult.Value.Id;
                    if (parentId is null) rootIncId = incId;
                    
                    try { 
                        await Task.WhenAll(result.SideEffects.Select(effect => _behavior(effect, ct)));
                    }
                    catch (AggregateException x) {
                        LogError(incId, "ProcessLogicResult", "SideEffect", null, x);
                    }

                    foreach (var derivedFact in result.DerivedFacts) {
                        RunLogic(derivedFact, cognitionResult.Value.Id, 1);
                    }
                }
                else {
                    switch (cognitionResult.Error) {
                        case Error.CognitionError x:
                            if (parentId is null) rootIncId = x.Increment.Id;
                            parentId = x.Increment.Id;
                            LogError(x.Increment.Id, "ProcessLogicResult", "TryReflect", x.Error);
                            break;
                        default:
                            LogError(aFact, "ProcessLogicResult", "TryReflect", cognitionResult.Error);
                            break;
                    }

                    RunLogic(aFact, parentId, attempt+1);
                }
            }

            async ValueTask<Result<Increment>> TryIncrement(
                Entity aFact, IncrementId? parentId, int attempt, long startMilliseconds,
                LogicEngine<TModel, TMsg, TSideEffect>.LogicResult output) {
                
                try {
                    var startCommitMilliseconds = stopWatch?.ElapsedMilliseconds;
                    var reflectTime = DateTime.UtcNow;
                    var incId = IncrementId.NewId();
                    var premisesArr = 
                        output.Inputs
                            .Where(pair => pair.Value.Item3)
                            .Select(pair => new EntityStampKey(pair.Key, pair.Value.Item2)).ToArray();
                    
                    var outputsArr = 
                        output.Outputs.Values
                            // skip output if it is equal to input
                            .Where(entity => !(output.Inputs.TryGetValue(entity.Id, out var pair) 
                                                && Equals(entity.Value, pair.Item1.Value)))
                            .Select(mem => new EntityStamp(mem, incId)).ToArray();

                    var commitResult = await _dataEngine.TryCommit(premisesArr, outputsArr.Select(x => x.Key), ct);
                    var endCommitMilliseconds = stopWatch?.ElapsedMilliseconds;

                    var status = IncrementStatus.Unknown;
                    var detailsDict = new Dictionary<string, string>();
                    List<Error>? errors = null;
                    if (commitResult.IsOk) {
                        status = IncrementStatus.Success;
                        // upload outputs only after success commit
                        var uploadOutputsResult = await _dataEngine.Upload(outputsArr, _serializer, ct, commitResult.IsError);
                        if (uploadOutputsResult.IsError) {
                            status = IncrementStatus.Lost;
                            incId = incId.AsLost();
                            errors ??= new List<Error>();
                            errors.Add(uploadOutputsResult.Error);
                        }
                    }
                    else {
                        if (commitResult.Error is Error.CommitError err) {
                            status = IncrementStatus.Fail;
                            incId = incId.AsFail();
                            detailsDict[Increment.DetailsCommitErrors] =
                                string.Join(' ', err.Conflicts.Select(x => x.ToString()));
                        }
                        else {
                            status = IncrementStatus.Lost;
                            incId = incId.AsLost();
                        }
                        errors ??= new List<Error>();
                        errors.Add(commitResult.Error);
                    }
                    
                    var factEntity = new EntityStamp(aFact, incId);
                    var increment = new Increment(
                        incId, 
                        parentId ?? IncrementId.None, 
                        status,
                        fact.Id,
                        output.Inputs
                            .ToList()
                            .Select(pair => new KeyValuePair<EntityStampKey,bool>(
                                new EntityStampKey(pair.Key, pair.Value.Item2), pair.Value.Item3))
                            .ToList(),
                        outputsArr.Select(x => x.Key.EntId).ToArray(),
                        output.Constants,
                        output.SideEffects.Select(x => x?.ToString() ?? "???").ToList(),
                        detailsDict);
                    
                    detailsDict[Increment.DetailsIncrementTime] = reflectTime.ToFileTimeUtc().ToString();
                    detailsDict[Increment.DetailsAttempt] = attempt.ToString();
                    detailsDict[Increment.DetailsLogicDuration] = (startCommitMilliseconds - startMilliseconds).ToString()!;
                    detailsDict[Increment.DetailsCommitDuration] = (endCommitMilliseconds - startCommitMilliseconds).ToString()!;
                    detailsDict[Increment.DetailsUploadDuration] = (stopWatch!.ElapsedMilliseconds - endCommitMilliseconds).ToString()!;
                    
                    var uploadFactResult = await _dataEngine.Upload(new[] {factEntity}, _serializer, ct, true);
                    if (uploadFactResult.IsError) {
                        errors ??= new List<Error>();
                        errors.Add(uploadFactResult.Error);
                        _logger.LogError($"{incId} Upload fact error: {uploadFactResult.Error}");
                    }

                    var sysEntities = parentId.HasValue
                        ? new [] {Increment.CreateEntStamp(increment), Increment.CreateChildEntStamp(increment.Id, parentId.Value)}
                        : new [] {Increment.CreateEntStamp(increment)}; 
                    var uploadSysEntitiesResult = await _dataEngine.Upload(sysEntities, _systemSerializer, ct, true);
                    if (uploadSysEntitiesResult.IsError) {
                        errors ??= new List<Error>();
                        errors.Add(uploadSysEntitiesResult.Error);
                        _logger.LogError($"{incId} Upload sysEntities error: {uploadSysEntitiesResult.Error}");
                    }
                    
                    if (status != IncrementStatus.Success) {
                        var error = errors != null
                            ? errors.Count > 1 
                                ? new Error.AggregateError(errors.ToArray()) 
                                : errors[0]
                            : Error.No; // should not be that
                        return Result<Increment>.CongnitionError(increment, error);
                    }

                    return new Result<Increment>(increment);
                }
                catch (TaskCanceledException) { throw; }
                catch (Exception e) { return Result<Increment>.Exception(e); }
            }
        }

        private void LogError(Entity fact, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {Fact} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "IncrementEngine", op, kind, fact, err);
            else _logger.LogError(exn,msg, "Funes", "IncrementEngine",  op, kind, fact, err);
        }

        private void LogError(IncrementId incId, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {IncId} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "IncrementEngine", op, kind, incId, err);
            else _logger.LogError(exn, msg, "Funes", "IncrementEngine",  op, kind, incId, err);
        }
    }
}