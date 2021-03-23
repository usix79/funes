using System;
using System.IO;
using Funes.Tests;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Fs.Tests {
    
    public class FileSystemRepoTests : AbstractRepoTests {
        private readonly ITestOutputHelper _testOutputHelper;

        public FileSystemRepoTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        protected override Mem.IRepository CreateRepo() {
            var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _testOutputHelper.WriteLine($"FS Repo path: {path}");
            return new FileSystemRepository(path);
        }
    }
}