using System;
using System.IO;
using System.Text;
using Funes.Impl;
using Funes.Indexes;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;


namespace Funes.Tests {
    public class IndexesTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public IndexesTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void EmptyIndexRecordEncoding() {

            var record = new IndexRecord();

            var size = IndexesHelpers.CalcSize(record);
            Assert.Equal(0, size);

            var data = IndexesHelpers.EncodeRecord(record);

            var reader = new IndexRecordsReader(data.Memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void IndexesRecordEncoding() {

            var record = new IndexRecord() {
                new (IndexOp.Kind.Update, "key1", "val1"),
                new (IndexOp.Kind.Remove, "key221","val-0")
            };

            var data = IndexesHelpers.EncodeRecord(record);

            var reader = new IndexRecordsReader(data.Memory);

            Assert.True(reader.MoveNext());
            Assert.Equal(record[0], reader.Current);
            Assert.True(reader.MoveNext());
            Assert.Equal(record[1], reader.Current);
            Assert.False(reader.MoveNext());
        }

        [Fact]
        public async void AddingFirstIndex() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var de = new StatelessDataEngine(repo, cache, tre, logger);

            var idxName = "testIdx";
            var key = "key1";
            var val = "value1";
            
            var ctx = new UpdateContext(de, idxName);

            var op = new IndexOp(IndexOp.Kind.Update, key, val);
            var registerResult = await ctx.RegisterOp(op, default);
            Assert.True(registerResult.IsOk, registerResult.Error.ToString());
            ctx.Update();

            Assert.Empty(ctx.RetrievedEntities);
            Assert.Single(ctx.UpdatedEntities);

            var rootId = IndexesHelpers.GetRootId(idxName);
            Assert.True(ctx.UpdatedEntities.TryGetValue(rootId, out var rootEntity));
            Assert.True(rootEntity.Value is IndexPage);
            if (rootEntity.Value is IndexPage root) {
                Assert.Equal(PageKind.Page, root.Kind);
                Assert.Equal(1, root.Count);
                Assert.Equal(0, root.CompareValue(0, val));
                Assert.Equal(0, root.CompareKey(0, key));
            }
        }
    }
}