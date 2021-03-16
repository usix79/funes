using System;
using System.IO;
using Funes.Tests;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Fs.Tests {
    
    public class FileSystemRepositoryTests : AbstractRepoTests {
        private readonly ITestOutputHelper _testOutputHelper;

        public FileSystemRepositoryTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        public override IRepository CreateRepo() {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _testOutputHelper.WriteLine($"FS Repo path: {path}");
            return new FileSystemRepository(path);
        }
    }
}