using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Funes.Impl;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class SysSerializerTests {
        private readonly ITestOutputHelper _testOutputHelper;

        public SysSerializerTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async void CognitionEncoding() {
            var incId = IncrementId.NewId();
            var parentIncId = IncrementId.NewId();
            var factStamp = CreateSimpleFact(0, "").ToStamp(IncrementId.NewFactId());
            var inputs = new Dictionary<EntityStampKey, bool> {
                {CreateRandomStampKey(), false}, {CreateRandomStampKey(), true}, {CreateRandomStampKey(), false}
                };
            var outputs = new [] {CreateRandomEntId(), CreateRandomEntId(), CreateRandomEntId()};
            var derivedFacts = new [] {CreateRandomEntId(), CreateRandomEntId(), CreateRandomEntId()};
            var constants = new Dictionary<string, string> {{"c1", "1"}, {"c2", "12"}, {"c3",  "123"}};
            var details = new Dictionary<string, string> {["d1"] = "a", ["d2"] = "ab", ["d3"] = "abc"};

            var cognition = new Increment(incId, parentIncId, IncrementStatus.Success, factStamp.Key,
                inputs.ToArray(), outputs, derivedFacts, constants.ToList(), details);

            var sysSer = new SystemSerializer();
            
            var stream = new MemoryStream();
            var encodingResult = await sysSer.Encode(stream, Increment.CreateEntId(incId), cognition);
            Assert.True(encodingResult.IsOk, encodingResult.Error.ToString());

            stream.Position = 0;
            var reader = new StreamReader(stream);
            _testOutputHelper.WriteLine($"JSON: {await reader.ReadToEndAsync()}");

            stream.Position = 0;
            var decodingResult = await sysSer.Decode(stream, Increment.CreateEntId(incId), encodingResult.Value);
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());
            Assert.IsType<Increment>(decodingResult.Value);
            if (decodingResult.Value is Increment decodedCognition) {
                Assert.Equal(cognition.Id, decodedCognition.Id);
                Assert.Equal(cognition.ParentId, decodedCognition.ParentId);
                Assert.Equal(cognition.Status, decodedCognition.Status);
                Assert.Equal(cognition.Fact, decodedCognition.Fact);
                Assert.Equal(cognition.Inputs, decodedCognition.Inputs);
                Assert.Equal(cognition.Outputs, decodedCognition.Outputs);
                Assert.Equal(cognition.DerivedFacts, decodedCognition.DerivedFacts);
                Assert.Equal(cognition.Constants, decodedCognition.Constants);
                Assert.Equal(cognition.Details, decodedCognition.Details);
            }
        }
    }
}