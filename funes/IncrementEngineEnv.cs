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
        public LogicEngineEnv<TModel, TMsg, TSideEffect> LogicEngineEnv { get; }
        public Behavior<TSideEffect> Behavior { get; }
        
        private readonly Stopwatch _stopwatch;

        public long ElapsedMilliseconds => _stopwatch.ElapsedMilliseconds;

        public IncrementEngineEnv(
            LogicEngineEnv<TModel, TMsg, TSideEffect> logicEngineEnv, 
            Behavior<TSideEffect> behavior,
            ISerializer serializer,
            IDataEngine de, 
            ILogger logger, 
            int maxAttempts = 3,
            int maxEventLogSize = 42) {
            (LogicEngineEnv, Behavior, Serializer, DataEngine, Logger, MaxAttempts, MaxEventLogSize) = 
                (logicEngineEnv, behavior, serializer, de, logger, maxAttempts, maxEventLogSize);

            _stopwatch = new Stopwatch();
            _stopwatch.Start();
        }
        
    }
}