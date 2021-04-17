using Funes.Impl;
using Microsoft.Extensions.Logging;

namespace Funes {
    
    public class LogicEngineEnv<TModel,TMsg,TSideEffect> {
        public LogicEngineEnv(ILogic<TModel, TMsg, TSideEffect> logic, ISerializer serializer, IDataSource dataSource,
            ILogger logger, ITracer<TModel, TMsg, TSideEffect> tracer, int iterationsLimit = 100500) {
            Logic = logic;
            Serializer = serializer;
            DataSource = dataSource;
            Logger = logger;
            Tracer = tracer;
            IterationsLimit = iterationsLimit;
        }

        public int IterationsLimit { get; }
        public ILogger Logger { get; }
        public ITracer<TModel,TMsg,TSideEffect> Tracer { get; }
        public ISerializer Serializer { get; }
        public ISerializer SysSerializer { get; } = new SystemSerializer();
        public IDataSource DataSource { get; }
        public ILogic<TModel,TMsg,TSideEffect> Logic { get; }
        
    }
}