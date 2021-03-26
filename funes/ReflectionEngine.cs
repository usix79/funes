using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes {
    public class ReflectionEngine<TState,TMsg,TSideEffect> {
        private readonly int _maxAttempts;
        private readonly ISerializer _serializer;
        private readonly ISerializer _systemSerializer = new SystemSerializer();
        private readonly IDataSource _ds;
        private readonly ILogger _logger;
        private readonly LogicEngine<TState, TMsg, TSideEffect> _logicEngine;
        private readonly Behaviour<TSideEffect> _behaviour;

        public ReflectionEngine(
                LogicEngine<TState, TMsg, TSideEffect> logicEngine, 
                Behaviour<TSideEffect> behaviour,
                ISerializer serializer,
                IDataSource ds, 
                ILogger logger, 
                int maxAttempts = 3) {
            (_logicEngine, _behaviour, _serializer, _ds, _logger, _maxAttempts) = 
                (logicEngine, behaviour, serializer,ds, logger, maxAttempts);
        }
        
        private readonly struct LogicItem {
            public Entity Fact { get; init; }
            public ReflectionId? ParentId { get; init; }
            public int Attempt { get; init; }
            
            public long StartMilliseconds { get; init; }
            public Task<Result<LogicEngine<TState,TMsg,TSideEffect>.Output>> Task { get; init; }
        }

        public async Task<Result<ReflectionId>> Run(Entity fact, CancellationToken ct = default) {
            
            var rootRid = ReflectionId.None;
            var stopWatch = Stopwatch.StartNew();
            
            LinkedList<LogicItem> logicItems = new ();

            try {
                RunLogic(fact, null, 1);

                while (logicItems.First != null) {
                    await Task.WhenAny(logicItems.Select(holder => holder.Task));
                    ct.ThrowIfCancellationRequested();
                    await CheckLogicResults();
                }

                return new Result<ReflectionId>(rootRid);
            }
            catch (Exception x) {
                _logger.LogCritical(x, "{Lib} {Obj} {Op} {Exn}", "Funes", "ReflectionEngine", "Loop", x);
                return Result<ReflectionId>.Exception(x);
            }
            finally {
                stopWatch.Stop();
            }
            
            void RunLogic(Entity aFact, ReflectionId? parentId, int attempt) {
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

            async ValueTask ProcessLogicResult(Entity aFact, ReflectionId? parentId, int attempt, 
                long startMilliseconds, LogicEngine<TState, TMsg, TSideEffect>.Output output) {

                var reflectionResult = await TryReflect(aFact, parentId, attempt, startMilliseconds, output);

                if (reflectionResult.IsOk) {
                    var rid = reflectionResult.Value.Id;
                    if (parentId is null) rootRid = rid;
                    try { 
                        await Task.WhenAll(output.SideEffects.Select(effect => _behaviour(effect, ct)));
                    }
                    catch (AggregateException x) {
                        LogError(rid, "ProcessLogicResult", "SideEffect", null, x);
                    }

                    foreach (var derivedFact in output.DerivedFacts) {
                        RunLogic(derivedFact.Value, reflectionResult.Value.Id, 1);
                    }
                }
                else {
                    switch (reflectionResult.Error) {
                        case Error.ReflectionError x:
                            LogError(x.Reflection.Id, "ProcessLogicResult", "TryReflect", x.Error);
                            break;
                        default:
                            LogError(aFact, "ProcessLogicResult", "TryReflect", reflectionResult.Error);
                            break;
                    }
                    RunLogic(aFact, parentId, attempt++);
                }
            }

            async ValueTask<Result<Reflection>> TryReflect(
                Entity aFact, ReflectionId? parentId, int attempt, long startMilliseconds,
                LogicEngine<TState, TMsg, TSideEffect>.Output output) {
                
                try {
                    var startCommitMilliseconds = stopWatch?.ElapsedMilliseconds;
                    var reflectTime = DateTime.UtcNow;
                    var rid = ReflectionId.NewId();
                    var premisesArr = 
                        output.Premises.Select(pair => new EntityStampKey(pair.Key, pair.Value.Item2)).ToArray();
                    
                    var conclusionsArr = 
                        output.Conclusions.Values
                            // skip conclusion if it is equal to premise
                            .Where(entity => !(output.Premises.TryGetValue(entity.Id, out var pair) 
                                                && Equals(entity.Value, pair.Item1.Value)))
                            .Select(mem => new EntityStamp(mem, rid)).ToArray();

                    var commitResult = await _ds.Commit(premisesArr, conclusionsArr.Select(x => x.Key), ct);
                    var endCommitMilliseconds = stopWatch?.ElapsedMilliseconds;
                    var status = commitResult.IsOk ? ReflectionStatus.Truth : ReflectionStatus.Fallacy;
                    
                    var uploadConclusionsResult = await _ds.Upload(conclusionsArr, _serializer, ct);
                    if (uploadConclusionsResult.IsError) {
                        status = ReflectionStatus.Lost;
                        if (commitResult.IsOk) {
                            await _ds.Rollback(commitResult.Value, ct);
                        }
                    }
                    
                    var detailsDict = new Dictionary<string, string>();

                    var factEntity = new EntityStamp(aFact, rid);
                    var reflection = new Reflection(rid, parentId ?? ReflectionId.None, status,
                        fact.Id,
                        premisesArr,
                        conclusionsArr.Select(x => x.Key.Eid).ToArray(),
                        output.Constants,
                        output.SideEffects.Select(x => x?.ToString()).ToList().AsReadOnly(),
                        detailsDict);
                    
                    detailsDict[Reflection.DetailsReflectTime] = reflectTime.ToFileTimeUtc().ToString();
                    detailsDict[Reflection.DetailsAttempt] = attempt.ToString();
                    detailsDict[Reflection.DetailsLogicDuration] = (startCommitMilliseconds - startMilliseconds).ToString()!;
                    detailsDict[Reflection.DetailsCommitDuration] = (endCommitMilliseconds - startCommitMilliseconds).ToString()!;
                    detailsDict[Reflection.DetailsUploadDuration] = (stopWatch!.ElapsedMilliseconds - endCommitMilliseconds).ToString()!;

                    var sysEntities = new List<EntityStamp>(5); 
                    sysEntities.Add(new EntityStamp(new Entity(Reflection.CreateEntityId(rid), reflection), rid));
                    
                    if (parentId.HasValue) {
                        var eid = Reflection.CreateChildrenEntityId(parentId.Value);
                        sysEntities.Add(new EntityStamp(new Entity(eid, null!), rid));
                    }
                    
                    var uploadFactResult = await _ds.Upload(new[] {factEntity}, _serializer, ct, true);
                    var uploadSysEntitiesResult = await _ds.Upload(sysEntities, _systemSerializer, ct, true);

                    return status == ReflectionStatus.Truth && uploadSysEntitiesResult.IsOk 
                        ?  new Result<Reflection>(reflection)
                        : Result<Reflection>.ReflectionError(reflection, 
                            new Error.AggregateError(uploadConclusionsResult.Error, uploadFactResult.Error,
                                uploadSysEntitiesResult.Error));
                }
                catch (TaskCanceledException) { throw; }
                catch (Exception e) { return Result<Reflection>.Exception(e); }
            }
        }

        private void LogError(Entity fact, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {Fact} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "ReflectionEngine", op, kind, fact, err);
            else _logger.LogError(exn,msg, "Funes", "ReflectionEngine",  op, kind, fact, err);
        }

        private void LogError(ReflectionId rid, string op, string kind, Error? err = null, Exception? exn = null) {
            var msg = "{Lib} {Obj}, {Op} {Kind} {Rid} {Err}";
            if (exn == null && err is Error.ExceptionError xErr) exn = xErr.Exn;
        
            if (exn is null) _logger.LogError(msg, "Funes", "ReflectionEngine", op, kind, rid, err);
            else _logger.LogError(exn, msg, "Funes", "ReflectionEngine",  op, kind, rid, err);
        }

    }
}