using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Funes {
    
    public class IncrementEngineEnv<TModel, TMsg, TSideEffect> {
        
        public int MaxAttempts { get; }
        public ILogger Logger { get; }
        public ISerializer Serializer { get; }
        public IDataEngine DataEngine { get; }
        public LogicEngine<TModel, TMsg, TSideEffect> LogicEngine { get; }
        public Behavior<TSideEffect> Behavior { get; }

        public ISerializer SystemSerializer { get; } = new SystemSerializer();

        private readonly Stopwatch _stopwatch;

        public long ElapsedMilliseconds => _stopwatch.ElapsedMilliseconds;

        public IncrementEngineEnv(
            LogicEngine<TModel, TMsg, TSideEffect> logicEngine, 
            Behavior<TSideEffect> behavior,
            ISerializer serializer,
            IDataEngine de, 
            ILogger logger, 
            int maxAttempts = 3) {
            (LogicEngine, Behavior, Serializer, DataEngine, Logger, MaxAttempts) = 
                (logicEngine, behavior, serializer, de, logger, maxAttempts);

            _stopwatch = new Stopwatch();
            _stopwatch.Start();
        }
        
    }
}