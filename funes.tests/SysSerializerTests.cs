using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            var cid = CognitionId.NewId();
            var parentCid = CognitionId.NewId();
            var fact = CreateSimpleFact(0, "");
            var inputs = new Dictionary<EntityStampKey, bool> {
                {CreateRandomStampKey(), false}, {CreateRandomStampKey(), true}, {CreateRandomStampKey(), false}
                };
            var outputs = new EntityId[] {CreateRandomEid(), CreateRandomEid(), CreateRandomEid()};
            var constants = new Dictionary<string, string> {{"c1", "1"}, {"c2", "12"}, {"c3",  "123"}};
            var sideEffects = new List<string> {"effect1", "effect2", "effect3"};
            var details = new Dictionary<string, string> {["d1"] = "a", ["d2"] = "ab", ["d3"] = "abc"};

            var cognition = new Cognition(cid, parentCid, CognitionStatus.Truth, fact.Id,
                inputs.ToList(), outputs, constants.ToList(), sideEffects, details);

            var sysSer = new SystemSerializer();
            
            var stream = new MemoryStream();
            var encodingResult = await sysSer.Encode(stream, Cognition.CreateEntityId(cid), cognition);
            Assert.True(encodingResult.IsOk, encodingResult.Error.ToString());

            stream.Position = 0;
            var reader = new StreamReader(stream);
            _testOutputHelper.WriteLine($"JSON: {await reader.ReadToEndAsync()}");

            stream.Position = 0;
            var decodingResult = await sysSer.Decode(stream, Cognition.CreateEntityId(cid), encodingResult.Value);
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());
            Assert.IsType<Cognition>(decodingResult.Value);
            if (decodingResult.Value is Cognition decodedCognition) {
                Assert.Equal(cognition.Id, decodedCognition.Id);
                Assert.Equal(cognition.ParentId, decodedCognition.ParentId);
                Assert.Equal(cognition.Status, decodedCognition.Status);
                Assert.Equal(cognition.Fact, decodedCognition.Fact);
                Assert.Equal(cognition.Inputs, decodedCognition.Inputs);
                Assert.Equal(cognition.Outputs, decodedCognition.Outputs);
                Assert.Equal(cognition.Constants, decodedCognition.Constants);
                Assert.Equal(cognition.SideEffects, decodedCognition.SideEffects);
                Assert.Equal(cognition.Details, decodedCognition.Details);
            }
        }
    }
}