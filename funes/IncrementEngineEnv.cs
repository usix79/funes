using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Funes.Impl;

namespace Funes {
    
    public class IncrementEngineEnv<TModel, TMsg, TSideEffect> {
        
        public int MaxAttempts { get; }
        public int MaxEventLogSize { get; }
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
            int maxAttempts = 3,
            int maxEventLogSize = 42) {
            (LogicEngine, Behavior, Serializer, DataEngine, Logger, MaxAttempts, MaxEventLogSize) = 
                (logicEngine, behavior, serializer, de, logger, maxAttempts, maxEventLogSize);

            _stopwatch = new Stopwatch();
            _stopwatch.Start();
        }
        
    }
}