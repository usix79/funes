using Funes.Impl;

namespace Funes.Tests {
    public class SimpleCacheTests : AbstractCacheTests {
        protected override ICache CreateCache() => new SimpleCache();
    }
}