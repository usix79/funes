using System;
using Funes.Tests;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Funes.Redis.Tests {
    
    public class RedisCacheTests: AbstractCacheTests {
        private const string ConnectionStringEnvName = "FUNES_REDIS_TESTS_CS";
        
        private readonly ITestOutputHelper _testOutputHelper;
        
        private ILogger Logger() => XUnitLogger.CreateLogger(_testOutputHelper);

        public RedisCacheTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        protected override ICache CreateCache() {
            return new RedisCache(ResolveConnectionString(), Logger());
        }

        private string ResolveConnectionString() =>
            Environment.GetEnvironmentVariable(ConnectionStringEnvName) ?? "localhost";
    }
}