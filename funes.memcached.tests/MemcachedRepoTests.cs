using System;
using Funes.Tests;
using Xunit;

namespace Funes.Memcached.Tests {
    
    public class MemcachedRepoTests : AbstractRepoTests {
        public override IRepository CreateRepo() {
            var realRepo = new InMemoryRepository();

            var host = Environment.GetEnvironmentVariable("FUNES_TEST_MEMCACHED_HOST") ?? "localhost";
            var port = Environment.GetEnvironmentVariable("FUNES_TEST_MEMCACHED_PORT") ?? "11211";
            
            return new MemcachedRepository(realRepo, host, int.Parse(port));
        }
    }
}