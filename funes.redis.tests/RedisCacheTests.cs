using System;
using Funes.Tests;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Funes.Redis.Tests {
    
    public class RedisCacheTests: AbstractCacheTests {
        
        private readonly ITestOutputHelper _testOutputHelper;
        
        private ILogger Logger() => XUnitLogger.CreateLogger(_testOutputHelper);

        public RedisCacheTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        protected override ICache CreateCache() {
            return new RedisCache(Helpers.ResolveConnectionString(), Logger());
        }

    }
}