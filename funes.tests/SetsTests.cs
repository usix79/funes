using System;
using System.Data;
using System.IO;
using System.Text;
using Funes.Sets;
using Microsoft.Toolkit.HighPerformance;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Tests {
    
    public class SetsTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public SetsTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }


        [Fact]
        public void EmptySetsRecordEncoding() {

            var record = new SetRecord();

            var size = SetsHelpers.CalcSize(record);
            Assert.Equal(0, size);

            var data = SetsHelpers.EncodeRecord(record);

            var reader = new SetRecordsReader(data.Memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void SetsRecordEncoding() {

            var record = new SetRecord() {
                new (SetOp.Kind.Add, "tag1"),
                new (SetOp.Kind.ReplaceWith, "tag22")
            };

            var data = SetsHelpers.EncodeRecord(record);

            var reader = new SetRecordsReader(data.Memory);

            Assert.True(reader.MoveNext());
            Assert.Equal(record[0], reader.Current);
            Assert.True(reader.MoveNext());
            Assert.Equal(record[1], reader.Current);
            Assert.False(reader.MoveNext());
        }
        
        [Fact]
        public async void SnapshotEncoding() {
            var snapshot = new SetSnapshot();
            snapshot.Add("tag1");
            snapshot.Add("_tagЫ");
            snapshot.Add("tag144");
            snapshot.Add("bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla");
            snapshot.Add("");

            var eid = SetsHelpers.GetSnapshotId("testSet");
            
            var data = SetsHelpers.EncodeSnapshot(snapshot);

            var reader = new StreamReader(data.Memory.AsStream(), Encoding.Unicode);
            _testOutputHelper.WriteLine($"DATA: {await reader.ReadToEndAsync()}");

            var decodingResult = SetsHelpers.DecodeSnapshot(data); 
            Assert.True(decodingResult.IsOk, decodingResult.Error.ToString());

            var decodedSnapshot = decodingResult.Value;
            Assert.Equal(snapshot.Count, decodedSnapshot.Count);
            foreach (var tag in decodedSnapshot) {
                Assert.True(snapshot.Contains(tag), tag);
            }
        }
        
    }
}