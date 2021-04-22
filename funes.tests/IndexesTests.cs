using System;
using Funes.Impl;
using Funes.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Tests {
    public class IndexesTests {
        
        private readonly ITestOutputHelper _testOutputHelper;

        public IndexesTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void EmptyIndexRecordEncoding() {

            var record = new IndexRecord();

            var size = RecordBuilder.CalcSize(record);
            Assert.Equal(0, size);

            var data = RecordBuilder.EncodeRecord(record);

            var reader = new IndexRecordsReader(data.Memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void IndexesRecordEncoding() {

            var record = new IndexRecord() {
                new (IndexOp.Kind.Update, "key1", "val1"),
                new (IndexOp.Kind.Remove, "key221","val-0")
            };

            var data = RecordBuilder.EncodeRecord(record);

            var reader = new IndexRecordsReader(data.Memory);

            Assert.True(reader.MoveNext());
            Assert.Equal(record[0], reader.Current);
            Assert.True(reader.MoveNext());
            Assert.Equal(record[1], reader.Current);
            Assert.False(reader.MoveNext());
        }

        [Fact]
        public void EmptyIndexPage() {
            var page = IndexPageHelpers.EmptyPage;
            Assert.Equal(IndexPage.Kind.Page, page.PageKind);
            Assert.Equal(0, page.ItemsCount);
        }

        [Fact]
        public void IndexPageWithSingleItem() {
            var key = "key1";
            var value = "value1";

            var size = IndexPageHelpers.SizeOfEmptyPage + IndexPageHelpers.CalcPageItemSize(key, value);
            var memory = new Memory<byte>(new byte[size]);
            IndexPageHelpers.WriteHead(memory, IndexPage.Kind.Table, 1);
            IndexPageHelpers.AppendItem(memory, key, value);

            var page = new IndexPage(EntityId.None, new BinaryData("bin", memory));
            Assert.Equal(IndexPage.Kind.Table, page.PageKind);
            Assert.Equal(1, page.ItemsCount);
            Assert.Equal(key, page.GetKeyAt(0));
            Assert.Equal(value, page.GetValueAt(0));
        }

        [Fact]
        public void IndexPageWithMultipleItems() {
            var items = new (string, string)[] {
                ("key1", "value1"),
                ("key2", "Bla-Bla-Bla"),
                ("keyЯЯЯЯЯЯ", "valueЫЫЫЫЫЫЫЫЫы"),
                ("aakdkd", ""),
                ("", "sss"),
                ("", ""),
            };

            var size = IndexPageHelpers.SizeOfEmptyPage;
            foreach (var (key, value) in items)
                size += IndexPageHelpers.CalcPageItemSize(key, value);
            
            var memory = new Memory<byte>(new byte[size]);
            IndexPageHelpers.WriteHead(memory, IndexPage.Kind.Table, items.Length);
            foreach (var (key, value) in items)
                IndexPageHelpers.AppendItem(memory, key, value);

            var page = new IndexPage(EntityId.None, new BinaryData("bin", memory));
            Assert.Equal(IndexPage.Kind.Table, page.PageKind);
            Assert.Equal(items.Length, page.ItemsCount);
            for (var i = 0; i < items.Length; i++) {
                var (key, value) = items[i];
                Assert.Equal(key, page.GetKeyAt(i));
                Assert.Equal(value, page.GetValueAt(i));
            }
        }

        [Fact]
        public async void AddingFirstIndex() {
            var repo = new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            var logger = XUnitLogger.CreateLogger(_testOutputHelper);
            var de = new StatelessDataEngine(repo, cache, tre, logger);
            var ds = new DataSource(de);

            var idxName = "testIdx";
            var key = "key1";
            var val = "value1";
            
            var record = new IndexRecord {
                new (IndexOp.Kind.Update, key, val)
            };
            var recordData = RecordBuilder.EncodeRecord(record);
            var eventLog = new EventLog(IncrementId.None, IncrementId.None, recordData.Memory);

            var updateResult = await IndexesModule.UpdateIndex(logger, ds, idxName, eventLog, 10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var (newPages, newKeys) = updateResult.Value;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            Assert.Equal(1, root.ItemsCount);
            Assert.Equal(val, root.GetValueAt(0));
            Assert.Equal(key, root.GetKeyAt(0));
        }
    }
}