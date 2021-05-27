using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Funes.Fs;
using Microsoft.Extensions.Logging;
using Funes.Impl;
using Funes.Redis;
using Funes.S3;
using funes.sample.Domain;

namespace funes.sample {
    
    using Funes;
    
    public class App {
        public enum ConnectionType {
            InMemory,
            FileSystem,
            Aws
        };

        public ConnectionType CurrentConnectionType { get; set; } = ConnectionType.InMemory;
        public string CurrentFileSystemRoot { get; set; } = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        public string CurrentBucket { get; set; } = "funes-sample";
        public string CurrentRedisAddress { get; set; } = "localhost:6379";

        private readonly ILogger _logger = LoggerFactory.Create(builder => builder.AddConsole()).CreateLogger<App>();

        private ITracer<Model, Message, SideEffect> _tracer =
            new SimpleTracer<Model, Message, SideEffect>();

        private IRepository _repo;
        private ICache _cache;
        private ITransactionEngine _tre;
        private IDataEngine _de;
        private ISerializer _ser = new SampleSerializer();
        private readonly ILogic<Model, Message, SideEffect> _logic = new Logic();
        private LogicEngineEnv<Model, Message, SideEffect> _logicEngineEnv;
        private IncrementEngineEnv<Model, Message, SideEffect> _incEngineEnv;
        
        public static readonly App Instance = new ();
        
        public bool IsInitialized { get; private set; }


        public DataContext CrateContext() =>
            new DataContext(_de, _ser);
        public void InitializeInMemory() {
            CurrentConnectionType = ConnectionType.InMemory;
            _repo = new SimpleRepository();
            _cache = new SimpleCache();
            _tre = new SimpleTransactionEngine();
            _de = new StatelessDataEngine(_repo, _cache, _tre, _logger);
            _logicEngineEnv = new LogicEngineEnv<Model, Message, SideEffect>(_logic, _logger, _tracer);
            _incEngineEnv = new IncrementEngineEnv<Model, Message, SideEffect>(
                _logicEngineEnv, Behavior, _ser, _de, _logger);
            IsInitialized = true;
        }

        public void InitializeFileSystem(string fsRoot) {
            CurrentConnectionType = ConnectionType.FileSystem;
            CurrentFileSystemRoot = fsRoot;
            _repo = new FileSystemRepository(fsRoot);
            _cache = new SimpleCache();
            _tre = new SimpleTransactionEngine();
            _de = new StatelessDataEngine(_repo, _cache, _tre, _logger);
            _logicEngineEnv = new LogicEngineEnv<Model, Message, SideEffect>(_logic, _logger, _tracer);
            _incEngineEnv = new IncrementEngineEnv<Model, Message, SideEffect>(
                _logicEngineEnv, Behavior, _ser, _de, _logger);
            IsInitialized = true;
        }

        public void InitializeAws(string bucketName, string redisAddress) {
            CurrentConnectionType = ConnectionType.Aws;
            CurrentBucket = bucketName;
            CurrentRedisAddress = redisAddress;
            _repo = new S3Repository(bucketName, "sample");
            _cache = new RedisCache(redisAddress, _logger);
            _tre = new RedisTransactionEngine(redisAddress, _logger);
            _de = new StatelessDataEngine(_repo, _cache, _tre, _logger);
            _logicEngineEnv = new LogicEngineEnv<Model, Message, SideEffect>(_logic, _logger, _tracer);
            _incEngineEnv = new IncrementEngineEnv<Model, Message, SideEffect>(
                _logicEngineEnv, Behavior, _ser, _de, _logger);
            IsInitialized = true;
        }

        public ValueTask<Result<Void>> Behavior(IncrementId incId, SideEffect effect, CancellationToken ct) => 
            ValueTask.FromResult(new Result<Void>(Void.Value));

        public async ValueTask<Result<Void>> UploadTrigger(EntityEntry triggerEntry) {
            var data = _ser.Encode(triggerEntry.Eid, triggerEntry.Value);
            if (data.IsError) return new Result<Void>(data.Error); 
            var stamp = new BinaryStamp(triggerEntry.Key, data.Value);
            var uploadResult = await _de.Upload(stamp, default, true);
            return uploadResult;
        }

        public async ValueTask<Result<Void>> RunIncrement(Entity trigger) {
            var triggerEntry = EntityEntry.Ok(trigger, IncrementId.NewTriggerId());

            var uploadResult = await UploadTrigger(triggerEntry);
            if (uploadResult.IsError) return uploadResult;

            var incResult = await IncrementEngine<Model, Message, SideEffect>.Run(_incEngineEnv, triggerEntry);
            return incResult.IsOk 
                ? new Result<Void>(Void.Value) 
                : new Result<Void>(incResult.Error);
        }

        public ValueTask<Result<Void>> PopulateSampleData() {
            var trigger = Helpers.CreateOperationPopulate();
            return RunIncrement(trigger);
        }

        public ValueTask<Result<Void>> LikeBook(string bookId) {
            var trigger = Helpers.CreateOperationLike(bookId);
            return RunIncrement(trigger);
        }
    }
}