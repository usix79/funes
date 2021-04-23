using System;
using System.Text;
using Xunit;

namespace Funes.Tests {
    public class UtilsTests {
        
        [Fact]
        public void BinaryCompare() {
            Assert.Equal(0, Utils.Binary.Compare("bla-bla-ЫЫЫ-123", Encoding.Unicode.GetBytes("bla-bla-ЫЫЫ-123")));
            Assert.True(0 > Utils.Binary.Compare("bla-ЫЫЫ", Encoding.Unicode.GetBytes("bla-bla-ЫЫЫ-123")));
            Assert.True(0 > Utils.Binary.Compare("bla-alb-bla-bla-ЫЫЫ", Encoding.Unicode.GetBytes("bla-bla-ЫЫЫ-123")));
            Assert.True(0 < Utils.Binary.Compare("foo", Encoding.Unicode.GetBytes("bla")));
            Assert.True(0 < Utils.Binary.Compare("foo", Encoding.Unicode.GetBytes("")));
            Assert.True(0 > Utils.Binary.Compare("", Encoding.Unicode.GetBytes("f00")));
            Assert.True(0 == Utils.Binary.Compare("", Encoding.Unicode.GetBytes("")));
        }

        [Fact]
        public void BinaryCompareParts() {
            Assert.True(0 == Utils.Binary.CompareParts("", "",Encoding.Unicode.GetBytes("")));
            Assert.True(0 < Utils.Binary.CompareParts("a", "",Encoding.Unicode.GetBytes("")));
            Assert.True(0 < Utils.Binary.CompareParts("", "a",Encoding.Unicode.GetBytes("")));
            Assert.True(0 > Utils.Binary.CompareParts("", "",Encoding.Unicode.GetBytes("a")));
            Assert.True(0 == Utils.Binary.CompareParts("abc", "123",Encoding.Unicode.GetBytes("abc123")));
            Assert.True(0 < Utils.Binary.CompareParts("abc", "123",Encoding.Unicode.GetBytes("abc")));
        }

    }
}