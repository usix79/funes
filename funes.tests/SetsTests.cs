using System;
using Funes.Sets;
using Xunit;

namespace Funes.Tests {
    
    public class SetsTests {

        [Fact]
        public void EmptySetsRecordEncoding() {

            var record = new SetRecord();

            var size = SetsHelpers.CalcSize(record);
            Assert.Equal(0, size);

            var memory = new Memory<byte>(new byte[size]);
            SetsHelpers.SerializeRecord(record, memory);

            var reader = new SetRecordsReader(memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void SetsRecordEncoding() {

            var record = new SetRecord() {
                new (SetOp.Kind.Add, "tag1"),
                new (SetOp.Kind.ReplaceWith, "tag22")
            };

            var size = SetsHelpers.CalcSize(record);
            var memory = new Memory<byte>(new byte[size]);
            SetsHelpers.SerializeRecord(record, memory);

            var reader = new SetRecordsReader(memory);

            Assert.True(reader.MoveNext());
            Assert.Equal(record[0], reader.Current);
            Assert.True(reader.MoveNext());
            Assert.Equal(record[1], reader.Current);
            Assert.False(reader.MoveNext());
        }
    }
}