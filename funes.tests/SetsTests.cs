using System;
using System.Collections.Generic;
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
            var set = new HashSet<string> {
                "tag1",
                "_tagЫ",
                "tag144",
                "bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla-bla",
                ""
            };

            var snapshot = SetSnapshot.FromSet(set);

            var reader = new StreamReader(snapshot.Data.Memory.AsStream(), Encoding.Unicode);
            _testOutputHelper.WriteLine($"DATA: {await reader.ReadToEndAsync()}");
            
            var decodedSet = snapshot.GetSet();
            Assert.Equal(set.Count, decodedSet.Count);
            foreach (var tag in decodedSet) {
                Assert.True(set.Contains(tag), tag);
            }
        }
        
    }
}