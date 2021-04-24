using Microsoft.Extensions.Logging;

namespace Funes {
    
    public class LogicEngineEnv<TModel,TMsg,TSideEffect> {
        public LogicEngineEnv(
            ILogic<TModel, TMsg, TSideEffect> logic, 
            ILogger logger, 
            ITracer<TModel, TMsg, TSideEffect> tracer, 
            int iterationsLimit = 100500) {
            Logic = logic;
            Logger = logger;
            Tracer = tracer;
            IterationsLimit = iterationsLimit;
        }

        public int IterationsLimit { get; }
        public ILogger Logger { get; }
        public ITracer<TModel,TMsg,TSideEffect> Tracer { get; }
        public ILogic<TModel,TMsg,TSideEffect> Logic { get; }
        
    }
}