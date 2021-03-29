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
            
            Assert.Equal(31556995200000L, CognitionId.MillisecondsBeforeFryReawakening(freezeDate));
            Assert.Equal(0L, CognitionId.MillisecondsBeforeFryReawakening(reawakenDate));
            Assert.Equal(30888283500000L, CognitionId.MillisecondsBeforeFryReawakening(funesBirthDate));
            
            Assert.Equal("31556995200000-qddneg", CognitionId.ComposeId(freezeDate, rand).Id);
            Assert.Equal("00000000000000-smetfg", CognitionId.ComposeId(reawakenDate, rand).Id);
            Assert.Equal("30888283500000-mijgma", CognitionId.ComposeId(funesBirthDate, rand).Id);
        }

        // [Fact]
        // public async void SingleReflectionTest() {
        //     var repo = new InMemoryRepository();
        //
        //     var factKey = new MemKey("facts", "fact1");
        //     var fact = TestHelpers.CreateRandomMem(factKey);
        //     var facts = new[] {fact};
        //
        //     var input1Key = new MemKey("kb/entity1", "id1");
        //     var input1Cid = CognitionId.NewId();
        //     var input1 = TestHelpers.CreateRandomMem(input1Key);
        //     var input2Key = new MemKey("kb/entity2", "id2");
        //     var input2Cid = CognitionId.NewId();
        //     var input2 = TestHelpers.CreateRandomMem(input2Key);
        //     var inputs = new[] {(input1, input1Cid), (input2, input2Cid)};
        //
        //     var output1Key = new MemKey("kb/entity1", "id1");
        //     var output1 = TestHelpers.CreateRandomMem(output1Key);
        //     var output2Key = new MemKey("kb/entity2", "id3");
        //     var output2 = TestHelpers.CreateRandomMem(output2Key);
        //     var output3Key = new MemKey("kb/entity3", "id1");
        //     var output3 = TestHelpers.CreateRandomMem(output3Key);
        //     var outputs = new[] {output1, output2, output3};
        //
        //     var cid = await Cognition.Reflect( repo, facts, inputs, outputs);
        //
        //     var cognition = repo.GetMem(Cognition.ReflectionKey, cid);
        //     
        //     
        // }
    }
}