using Funes.Tests;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Funes.Redis.Tests {
    public class RedisTreTests : AbstractTreTests{
        
        private readonly ITestOutputHelper _testOutputHelper;

        public RedisTreTests(ITestOutputHelper testOutputHelper) => _testOutputHelper = testOutputHelper;

        private ILogger Logger() => XUnitLogger.CreateLogger(_testOutputHelper);

        protected override ITransactionEngine CreateEngine() {
            return new RedisTransactionEngine(Helpers.ResolveConnectionString(), Logger());
        }
    }
}