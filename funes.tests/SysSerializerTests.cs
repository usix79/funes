using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Funes.Impl;
using Funes.Sets;
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
        public async void IncrementEncoding() {
            var incId = IncrementId.NewId();
            var factStamp = CreateSimpleFact(0, "").ToStamp(IncrementId.NewStimulusId());
            var args = new IncrementArgs();
            args.RegisterEntity(CreateRandomStampKey(), false);
            args.RegisterEntity(CreateRandomStampKey(), true);
            args.RegisterEntity(CreateRandomStampKey(), false);
            var outputs = new List<EntityId> {CreateRandomEntId(), CreateRandomEntId(), CreateRandomEntId()};
            var constants = new Dictionary<string, string> {{"c1", "1"}, {"c2", "12"}, {"c3",  "123"}};
            var details = new Dictionary<string, string> {["d1"] = "a", ["d2"] = "ab", ["d3"] = "abc"};

            var cognition = new Increment(incId, factStamp.Key, args, outputs, constants.ToList(), details.ToList());

            var sysSer = new SystemSerializer();
            
            var stream = new MemoryStream();
            var encodingResult = await sysSer.Encode(stream, Increment.CreateEntId(incId), cognition, default);
            Assert.True(encodingResult.IsOk, encodingResult.Error.ToString());

            stream.Position = 0;
            var reader = new StreamReader(stream);
            _testOutputHelper.WriteLine($"JSON: {await reader.ReadToEndAsync()}");

            stream.Position = 0;
            var decodingResult = await sysSer.Decode(stream, Increment.CreateEntId(incId), encodingResult.Value, default);
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());
            Assert.IsType<Increment>(decodingResult.Value);
            if (decodingResult.Value is Increment decodedCognition) {
                Assert.Equal(cognition.Id, decodedCognition.Id);
                Assert.Equal(cognition.FactKey, decodedCognition.FactKey);
                Assert.Equal(cognition.Args.Entities, decodedCognition.Args.Entities);
                Assert.Equal(cognition.Args.Events, decodedCognition.Args.Events);
                Assert.Equal(cognition.Outputs, decodedCognition.Outputs);
                Assert.Equal(cognition.Constants, decodedCognition.Constants);
                Assert.Equal(cognition.Details, decodedCognition.Details);
            }
        }

        [Fact]
        public async void SetSnapshotEncoding() {
            var snapshot = new SetSnapshot();
            snapshot.Add("tag1");
            snapshot.Add("_tagЫ");
            snapshot.Add("tag144");
            snapshot.Add("bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla");
            snapshot.Add("");

            var eid = SetsHelpers.GetSnapshotId("testSet");
            
            var sysSer = new SystemSerializer();
            var stream = new MemoryStream();
            var encodingResult = await sysSer.Encode(stream, eid, snapshot, default);
            Assert.True(encodingResult.IsOk, encodingResult.Error.ToString());

            stream.Position = 0;
            var reader = new StreamReader(stream, Encoding.Unicode);
            _testOutputHelper.WriteLine($"DATA: {await reader.ReadToEndAsync()}");

            stream.Position = 0;
            var decodingResult = await sysSer.Decode(stream, eid, encodingResult.Value, default);
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());
            Assert.IsType<SetSnapshot>(decodingResult.Value);

            if (decodingResult.Value is SetSnapshot decodedSnapshot) {
                Assert.Equal(snapshot.Count, decodedSnapshot.Count);
                foreach (var tag in decodedSnapshot) {
                    Assert.True(snapshot.Contains(tag), tag);
                }
            }

        }
    }
}