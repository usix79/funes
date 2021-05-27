using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Toolkit.HighPerformance;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class IncrementTests {
        private readonly ITestOutputHelper _testOutputHelper;

        public IncrementTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void IncrementEncoding() {
            var incId = IncrementId.NewId();
            var factStamp = EntityEntry.Ok(CreateSimpleTrigger(0, ""), IncrementId.NewTriggerId());
            var inputs = new List<Increment.InputEntity>() {
                new(CreateRandomStampKey(), false),
                new(CreateRandomStampKey(), true),
                new(CreateRandomStampKey(), false)
            };
            var inputEventLogs = new List<Increment.InputEventLog>();
            var outputs = new List<EntityId> {CreateRandomEntId(), CreateRandomEntId(), CreateRandomEntId()};
            var constants = new Dictionary<string, string> {{"c1", "1"}, {"c2", "12"}, {"c3",  "123"}};
            var details = new Dictionary<string, string> {["d1"] = "a", ["d2"] = "ab", ["d3"] = "abc"};

            var increment = new Increment(incId, factStamp.Key, inputs, inputEventLogs, outputs, constants.ToList(), details.ToList());

            var encodingResult = Increment.Encode(increment); 
            Assert.True(encodingResult.IsOk, encodingResult.Error.ToString());

            var reader = new StreamReader(encodingResult.Value.Memory.AsStream());
            _testOutputHelper.WriteLine($"JSON: {await reader.ReadToEndAsync()}");

            var decodingResult = Increment.Decode(encodingResult.Value); 
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());
            var decodedIncrement = decodingResult.Value;
            Assert.Equal(increment.Id, decodedIncrement.Id);
            Assert.Equal(increment.TriggerKey, decodedIncrement.TriggerKey);
            Assert.Equal(increment.Inputs, decodedIncrement.Inputs);
            Assert.Equal(increment.EventLogInputs, decodedIncrement.EventLogInputs);
            Assert.Equal(increment.Outputs, decodedIncrement.Outputs);
            Assert.Equal(increment.Constants, decodedIncrement.Constants);
            Assert.Equal(increment.Details, decodedIncrement.Details);
        }
    }
}