using System;
using Funes.Tests;
using Xunit;
using Xunit.Abstractions;

namespace Funes.S3.Tests {
    
    public class S3RepositoryTests : AbstractRepoTests {
        
        private readonly ITestOutputHelper _testOutputHelper;
        
        public S3RepositoryTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }
        
        public override IRepository CreateRepo() {
            var bucketName = Environment.GetEnvironmentVariable("FUNES_TEST_BUCKET") ?? "funes-tests";
            var prefix = Guid.NewGuid().ToString();
            
            _testOutputHelper.WriteLine($"S3 Repo bucketName {bucketName}, prefix {prefix}");
            return new S3Repository(bucketName, prefix);
        }
    }
}
