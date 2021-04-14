using System;
using Funes.Indexes;
using Xunit;

namespace Funes.Tests {
    
    public class IndexesTests {

        [Fact]
        public void EmptyIndexRecordEncoding() {

            var record = new IndexRecord();

            var size = IndexHelpers.CalcSize(record);
            Assert.Equal(0, size);

            var memory = new Memory<byte>(new byte[size]);
            IndexHelpers.Serialize(record, memory);

            var reader = new IndexRecordReader(memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void IndexRecordEncoding() {

            var record = new IndexRecord() {
                new (IndexOp.Kind.AddTag, "key1", "tag1"),
                new (IndexOp.Kind.ReplaceTags, "key2", "tag22")
            };

            var size = IndexHelpers.CalcSize(record);
            var memory = new Memory<byte>(new byte[size]);
            IndexHelpers.Serialize(record, memory);

            var reader = new IndexRecordReader(memory);

            Assert.True(reader.MoveNext());
            Assert.Equal(record[0], reader.Current);
            Assert.True(reader.MoveNext());
            Assert.Equal(record[1], reader.Current);
            Assert.False(reader.MoveNext());
        }
    }
}