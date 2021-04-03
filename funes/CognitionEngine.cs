using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class CognitionEngine<TModel,TMsg,TSideEffect> {
        private readonly int _maxAttempts;
        private readonly ILogger _logger;
        private readonly ISerializer _serializer;
        private readonly ISerializer _systemSerializer = new SystemSerializer();
        private readonly IDataEngine _dataEngine;
        private readonly LogicEngine<TModel, TMsg, TSideEffect> _logicEngine;
        private readonly Behavior<TSideEffect> _behavior;

        public CognitionEngine(
                LogicEngine<TModel, TMsg, TSideEffect> logicEngine, 
                Behavior<TSideEffect> behavior,
                ISerializer serializer,
                IDataEngine de, 
                ILogger logger, 
                int maxAttempts = 3) {
            (_logicEngine, _behavior, _serializer, _dataEngine, _logger, _maxAttempts) = 
                (logicEngine, behavior, serializer, de, logger, maxAttempts);
        }
        
        private readonly struct LogicItem {
            public Entity Fact { get; init; }
            public CognitionId? ParentId { get; init; }
            public int Attempt { get; init; }
            
            public long StartMilliseconds { get; init; }
            public Task<Result<LogicEngine<TModel,TMsg,TSideEffect>.LogicResult>> Task { get; init; }
        }

        public async Task<Result<CognitionId>> Run(Entity fact, CancellationToken ct = default) {
            
            var rootCid = CognitionId.None;
            var stopWatch = Stopwatch.StartNew();
            
            LinkedList<LogicItem> logicItems = new ();

            try {
                RunLogic(fact, null, 1);

                while (logicItems.First != null) {
                    await Task.WhenAny(logicItems.Select(holder => holder.Task));
                    ct.ThrowIfCancellationRequested();
                    await CheckLogicResults();
                }

                return new Result<CognitionId>(rootCid);
            }
            catch (Exception x) {
                _logger.LogCritical(x, "{Lib} {Obj} {Op} {Exn}", "Funes", "CognitionEngine", "Loop", x);
                return Result<CognitionId>.Exception(x);
            }
            finally {
                stopWatch.Stop();
                await _dataEngine.Flush();
            }
            
            void RunLogic(Entity aFact, CognitionId? parentId, int attempt) {
                if (attempt <= _maxAttempts) {
                    var task = _logicEngine.Run(aFact, null!, ct);
                    var item = new LogicItem {
                        Fact = aFact, 
                        ParentId = parentId, 
                        Attempt = attempt, 
                        StartMilliseconds = stopWatch!.ElapsedMilliseconds, 
                        Task = task
                    };
                    logicItems.AddLast(item);
                }
                else {
                    LogError(aFact, "RunLogic", "MaxAttempts");
                }
            }

            async ValueTask CheckLogicResults() {
                var node = logicItems.First;
                while (node != null) {
                    if (node.Value.Task.IsCompleted) {
                        logicItems.Remove(node);
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

            async ValueTask ProcessLogicResult(Entity aFact, CognitionId? parentId, int attempt, 
                long startMilliseconds, LogicEngine<TModel, TMsg, TSideEffect>.LogicResult result) {

                var cognitionResult = await TryCognize(aFact, parentId, attempt, startMilliseconds, result);
                
                if (cognitionResult.IsOk) {
                    var cid = cognitionResult.Value.Id;
                    if (parentId is null) rootCid = cid;
                    
                    try { 
                        await Task.WhenAll(result.SideEffects.Select(effect => _behavior(effect, ct)));
                    }
                    catch (AggregateException x) {
                        LogError(cid, "ProcessLogicResult", "SideEffect", null, x);
                    }

                    foreach (var derivedFact in result.DerivedFacts) {
                        RunLogic(derivedFact, cognitionResult.Value.Id, 1);
                    }
                }
                else {
                    switch (cognitionResult.Error) {
                        case Error.CognitionError x:
                            if (parentId is null) rootCid = x.Cognition.Id;
                            parentId = x.Cognition.Id;
                            LogError(x.Cognition.Id, "ProcessLogicResult", "TryReflect", x.Error);
                            break;
                        default:
                            LogError(aFact, "ProcessLogicResult", "TryReflect", cognitionResult.Error);
                            break;
                    }

                    RunLogic(aFact, parentId, attempt+1);
                }
            }

            async ValueTask<Result<Cognition>> TryCognize(
                Entity aFact, CognitionId? parentId, int attempt, long startMilliseconds,
                LogicEngine<TModel, TMsg, TSideEffect>.LogicResult output) {
                
                try {
                    var startCommitMilliseconds = stopWatch?.ElapsedMilliseconds;
                    var reflectTime = DateTime.UtcNow;
                    var cid = CognitionId.NewId();
                    var premisesArr = 
                        output.Inputs
                            .Where(pair => pair.Value.Item3)
                            .Select(pair => new EntityStampKey(pair.Key, pair.Value.Item2)).ToArray();
                    
                    var outputsArr = 
                        output.Outputs.Values
                            // skip conclusion if it is equal to premise
                            .Where(entity => !(output.Inputs.TryGetValue(entity.Id, out var pair) 
                                                && Equals(entity.Value, pair.Item1.Value)))
                            .Select(mem => new EntityStamp(mem, cid)).ToArray();

                    var commitResult = await _dataEngine.Commit(premisesArr, outputsArr.Select(x => x.Key), ct);
                    var endCommitMilliseconds = stopWatch?.ElapsedMilliseconds;
                    var status = commitResult.IsOk 
                        ? CognitionStatus.Truth 
                        : commitResult.Error is Error.TransactionError ? CognitionStatus.Fallacy : CognitionStatus.Lost;

                    var detailsDict = new Dictionary<string, string>();
                    if (status != CognitionStatus.Truth) {
                        cid = status == CognitionStatus.Fallacy ? cid.AsFallacy() : cid.AsLost();
                        outputsArr = outputsArr.Select(stamp => new EntityStamp(stamp.Entity, cid)).ToArray();

                        if (commitResult.Error is Error.TransactionError err) {
                            detailsDict[Cognition.DetailsCommitErrors] =
                                string.Join(' ', err.Conflicts.Select(x => x.ToString()));
                        } 
                    }
                    var uploadOutputsResult = await _dataEngine.Upload(outputsArr, _serializer, ct, commitResult.IsError);
                    if (uploadOutputsResult.IsError) {
                        status = CognitionStatus.Lost;
                        
                        cid = cid.AsLost();
                    }
                    
                    var factEntity = new EntityStamp(aFact, cid);
                    var cognition = new Cognition(cid, parentId ?? CognitionId.None, status,
                        fact.Id,
                        output.Inputs.ToList()
                            .Select(pair => new KeyValuePair<EntityStampKey,bool>(new EntityStampKey(pair.Key, pair.Value.Item2), pair.Value.Item3))
                            .ToList(),
                        outputsArr.Select(x => x.Key.Eid).ToArray(),
                        output.Constants,
                        output.SideEffects.Select(x => x?.ToString() ?? "???").ToList(),
                        detailsDict);
                    
                    detailsDict[Cognition.DetailsCognitionTime] = reflectTime.ToFileTimeUtc().ToString();
                    detailsDict[Cognition.DetailsAttempt] = attempt.ToString();
                    detailsDict[Cognition.DetailsLogicDuration] = (startCommitMilliseconds - startMilliseconds).ToString()!;
                    detailsDict[Cognition.DetailsCommitDuration] = (endCommitMilliseconds - startCommitMilliseconds).ToString()!;
                    detailsDict[Cognition.DetailsUploadDuration] = (stopWatch!.ElapsedMilliseconds - endCommitMilliseconds).ToString()!;

                    var sysEntities = new List<EntityStamp>(5); 
                    sysEntities.Add(new EntityStamp(new Entity(Cognition.CreateEntityId(cid), cognition), cid));
                    
                    if (parentId.HasValue) {
                        var eid = Cognition.CreateChildEntityId(parentId.Value);
                        sysEntities.Add(new EntityStamp(new Entity(eid, null!), cid));
                    }
                    
                    var uploadFactResult = await _dataEngine.Upload(new[] {factEntity}, _serializer, ct, true);
                    var uploadSysEntitiesResult = await _dataEngine.Upload(sysEntities, _systemSerializer, ct, true);

                    if (uploadFactResult.IsError) {
                        _logger.LogError($"{cid} Upload fact error: {uploadFactResult.Error}");
                    }
                    if (uploadSysEntitiesResult.IsError) {
                        _logger.LogError($"{cid} Upload sysEntities error: {uploadSysEntitiesResult.Error}");
                    }
                    
                    if (status == CognitionStatus.Truth) {
                        return new Result<Cognition>(cognition);
                    }
                    else {
                        var errors = new List<Error>();
                        if (commitResult.IsError) errors.Add(commitResult.Error);
                        if (uploadOutputsResult.IsError) errors.Add(uploadOutputsResult.Error);
                        var error = errors.Count > 1 ? new Error.AggregateError(errors.ToArray()) : errors[0];
                        return Result<Cognition>.CongnitionError(cognition, error);
                    }
                }
                catch (TaskCanceledException) { throw; }
                catch (Exception e) { return Result<Cognition>.Exception(e); }
            }
        }

        private void LogError(Entity fact, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {Fact} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "CognitionEngine", op, kind, fact, err);
            else _logger.LogError(exn,msg, "Funes", "CognitionEngine",  op, kind, fact, err);
        }

        private void LogError(CognitionId cid, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {Cid} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "CognitionEngine", op, kind, cid, err);
            else _logger.LogError(exn, msg, "Funes", "CognitionEngine",  op, kind, cid, err);
        }
    }
}