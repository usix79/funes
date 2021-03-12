using System;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Tests
{
    public class ReflectionTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public ReflectionTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void ReflectionIdTest() {
            var freezeDate = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var reawakenDate = new DateTimeOffset(3000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            var funesBirthDate = new DateTimeOffset(2021, 3, 10, 17, 15, 0, TimeSpan.Zero);
            var rand = new Random(42);
            
            Assert.Equal(31556995200000L, ReflectionId.MillisecondsBeforeFryReawakening(freezeDate));
            Assert.Equal(0L, ReflectionId.MillisecondsBeforeFryReawakening(reawakenDate));
            Assert.Equal(30888283500000L, ReflectionId.MillisecondsBeforeFryReawakening(funesBirthDate));
            
            Assert.Equal("31556995200000-qddneg", ReflectionId.ComposeId(freezeDate, rand).Id);
            Assert.Equal("0-smetfg", ReflectionId.ComposeId(reawakenDate, rand).Id);
            Assert.Equal("30888283500000-mijgma", ReflectionId.ComposeId(funesBirthDate, rand).Id);
        }
    }
}