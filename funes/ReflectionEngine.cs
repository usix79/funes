using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    
    public delegate Task<Result<(MemKey,ReflectionId)[]>> TrySetConclusions(IEnumerable<MemKey> premises, IEnumerable<MemKey> conclusions, CancellationToken ct);
    public delegate Task<Result<bool>> RollbackConclusions(IEnumerable<(MemKey,ReflectionId)> trySetResult, CancellationToken ct);
    public delegate Task<Result<bool>> Upload(IEnumerable<MemStamp> mems, CancellationToken ct);
    public delegate Task<Result<bool>> Behaviour<in TSideEffect>(TSideEffect sideEffect, CancellationToken ct);

    public class ReflectionEngine<TState,TMsg,TSideEffect> {
        
        private readonly int _maxAttempts;
        private readonly LogicEngine<TState, TMsg, TSideEffect> _logicEngine;
        private readonly TrySetConclusions _trySetConclusions;
        private readonly RollbackConclusions _rollbackConclusions;
        private readonly Upload _upload;
        private readonly Behaviour<TSideEffect> _behaviour;

        public ReflectionEngine(
                LogicEngine<TState, TMsg, TSideEffect> logicEngine,
                Behaviour<TSideEffect> behaviour, 
                TrySetConclusions trySetConclusions, 
                RollbackConclusions rollbackConclusions,
                Upload upload,
                int maxAttempts = 3) {
            _logicEngine = logicEngine;
            _behaviour = behaviour;
            _trySetConclusions = trySetConclusions;
            _upload = upload;
            _rollbackConclusions = rollbackConclusions;
            _maxAttempts = maxAttempts;
        }
        
        private readonly struct LogicItem {
            public Mem Fact { get; init; }
            public ReflectionId? ParentId { get; init; }
            public int Attempt { get; init; }
            public Task<Result<LogicEngine<TState,TMsg,TSideEffect>.Output>> Task { get; init; }
        }

        public async Task Run(Mem fact, CancellationToken ct = default) {

            LinkedList<LogicItem> logicItems = new();

            try {
                RunLogic(fact, null, 1);

                while (logicItems.First != null) {
                    await Task.WhenAny(logicItems.Select(holder => holder.Task));
                    await CheckLogicResults();
                }
            }
            catch (Exception x) {
                //
            }
            
            void RunLogic(Mem aFact, ReflectionId? parentId, int attempt) {
                if (attempt <= _maxAttempts) {
                    var task = _logicEngine.Run(aFact, ct);
                    var holder = new LogicItem {Fact = aFact, ParentId = parentId, Attempt = attempt, Task = task};
                    logicItems.AddLast(holder);
                }
            }

            async ValueTask CheckLogicResults() {
                var node = logicItems.First;
                while (node != null) {
                    if (node.Value.Task.IsCompleted) {
                        if (node.Value.Task.IsCompletedSuccessfully) {
                            var holder = node.Value;
                            await ProcessLogicResult(holder.Fact, holder.ParentId, holder.Attempt, holder.Task.Result);                            
                        }
                        
                        logicItems.Remove(node);
                    }
                    node = node.Next;
                }
            }

            async ValueTask ProcessLogicResult(Mem aFact, ReflectionId? parentId, int attempt,
                Result<LogicEngine<TState, TMsg, TSideEffect>.Output> result) {

                if (result.IsError) {
                    RunLogic(aFact, parentId, attempt++);
                }
                else {
                    var reflectionResult = await TryReflect(aFact, parentId, attempt, result.Value);

                    if (reflectionResult.IsOk) {
                        // TODO: handle errors
                        await Task.WhenAll(result.Value.SideEffects.Select(effect => _behaviour(effect, ct)));

                        foreach (var derivedFact in result.Value.DerivedFacts) {
                            RunLogic(derivedFact.Value, reflectionResult.Value.Id, 1);
                            // TODO upload chield information
                        }
                    }
                    else {
                        RunLogic(aFact, parentId, attempt++);
                        // TODO: upload error as mem
                    }
                }
            }

            async ValueTask<Result<Reflection>> TryReflect(
                Mem aFact, ReflectionId? parentId, int attempt,
                LogicEngine<TState, TMsg, TSideEffect>.Output output) {
                
                var reflectTime = DateTime.UtcNow;
                try {
                    var rid = ReflectionId.NewId();
                    var premisesArr = output.Premises.ToArray();
                    var conclusionsArr = output.Conclusions.Values.Select(mem => new MemStamp(mem, rid)).ToArray();
                    
                    var sotResult = await _trySetConclusions(premisesArr, conclusionsArr.Select(x => x.Key), ct);
                    var status = sotResult.IsOk ? ReflectionStatus.Truth : ReflectionStatus.Fallacy;

                    var uploadResult = await _upload(conclusionsArr, ct);
                    if (uploadResult.IsError) {
                        status = ReflectionStatus.Lost;
                        if (sotResult.IsOk) { // upload error, rollback conclusions
                            await _rollbackConclusions(sotResult.Value, ct);
                        }
                    }

                    var detailsDict = new Dictionary<string,string>();

                    var factMem = new MemStamp(aFact, rid);
                    var reflection = new Reflection(rid, parentId??ReflectionId.None, status, 
                        fact.Id, premisesArr, conclusionsArr.Select(x => x.Key.Id).ToArray(), detailsDict);
                    var reflectionMem = new MemStamp(new Mem(Reflection.CreateMemId(rid), reflection), rid);

                    detailsDict[Reflection.DetailsReflectTime] = reflectTime.ToFileTimeUtc().ToString();
                    detailsDict[Reflection.DetailsRepoTime] = DateTime.UtcNow.ToFileTimeUtc().ToString();
                    detailsDict[Reflection.DetailsAttempt] = attempt.ToString();

                    var result = await _upload(new []{factMem, reflectionMem}, ct);

                    return result.IsOk
                        ? new Result<Reflection>(reflection)
                        : Result<Reflection>.ReflectionError(reflection, result.Error);

                }
                catch (Exception e) {
                    return Result<Reflection>.Exception(e);
                }
            }
        }
    }
}