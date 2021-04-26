using System;
using Funes.Impl;
using Funes.Indexes;
using Microsoft.Extensions.Logging;
using NuGet.Frameworks;
using Xunit;
using Xunit.Abstractions;

namespace Funes.Tests {
    public class IndexesTests {
        
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly ILogger _logger;

        private IDataEngine CreateDataEngine(IRepository? repository = null) {
            var repo = repository ?? new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            return new StatelessDataEngine(repo, cache, tre, _logger);
        }
        
        public IndexesTests(ITestOutputHelper testOutputHelper) {
            _testOutputHelper = testOutputHelper;
            _logger = XUnitLogger.CreateLogger(_testOutputHelper);
        }

        [Fact]
        public void EmptyIndexRecordEncoding() {

            var record = new IndexRecord();

            var size = IndexRecord.Builder.CalcSize(record);
            Assert.Equal(0, size);

            var data = IndexRecord.Builder.EncodeRecord(record);

            var reader = new IndexRecord.Reader(data.Memory);
            Assert.False(reader.MoveNext());
        }
    
        [Fact]
        public void IndexesRecordEncoding() {

            var record = new IndexRecord() {
                new (IndexOp.Kind.Update, "key1", "val1"),
                new (IndexOp.Kind.Remove, "key221","val-0")
            };

            var data = IndexRecord.Builder.EncodeRecord(record);

            var reader = new IndexRecord.Reader(data.Memory);

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
            IndexPageHelpers.WriteHead(memory.Span, IndexPage.Kind.Table, 1);
            IndexPageHelpers.AppendItem(memory.Span, key, value);

            var page = new IndexPage(EntityId.None, new BinaryData("bin", memory));
            Assert.Equal(IndexPage.Kind.Table, page.PageKind);
            Assert.Equal(1, page.ItemsCount);
            Assert.Equal(key, page.GetKeyAt(0));
            Assert.Equal(value, page.GetValueAt(0));
        }

        [Fact]
        public void IndexPageWithMultipleItems() {
            var items = new [] {
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
            IndexPageHelpers.WriteHead(memory.Span, IndexPage.Kind.Table, items.Length);
            foreach (var (key, value) in items)
                IndexPageHelpers.AppendItem(memory.Span, key, value);

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
            var de = CreateDataEngine();
            var ds = new DataContext(de, new SimpleSerializer<Simple>());

            var idxName = "testIdx";
            var key = "key1";
            var val = "value1";
            
            var record = new IndexRecord {
                new (IndexOp.Kind.Update, key, val)
            };
            var recordData = IndexRecord.Builder.EncodeRecord(record);
            var eventLog = new EventLog(IncrementId.None, IncrementId.None, recordData.Memory);
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.BuildIndex(_logger, ds, idxName, eventOffset, eventLog, 10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            Assert.Equal(1, root.ItemsCount);
            Assert.Equal(val, root.GetValueAt(0));
            Assert.Equal(key, root.GetKeyAt(0));

            var keys = updateResult.Value.Keys;
            Assert.Single(keys);
            Assert.Equal("value1", keys["key1"].GetValue());
        }
        
        [Fact]
        public async void AddingManyToNewIndex() {
            var de = CreateDataEngine();
            var ds = new DataContext(de, new SimpleSerializer<Simple>());

            var idxName = "testIdx";
            
            var record = new IndexRecord {
                new (IndexOp.Kind.Update, "key1", "val1"),
                new (IndexOp.Kind.Remove, "key2", ""),
                new (IndexOp.Kind.Update, "key3", "val333"),
                new (IndexOp.Kind.Remove, "key3", ""),
                new (IndexOp.Kind.Update, "key3", "val444"),
                new (IndexOp.Kind.Remove, "key1", ""),
            };
            var recordData = IndexRecord.Builder.EncodeRecord(record);
            var eventLog = new EventLog(IncrementId.None, IncrementId.None, recordData.Memory);
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.BuildIndex(_logger, ds, idxName, eventOffset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            Assert.Equal(1, root.ItemsCount);
            Assert.Equal("val444", root.GetValueAt(0));
            Assert.Equal("key3", root.GetKeyAt(0));
            
            var keys = updateResult.Value.Keys;
            Assert.Single(keys);
            Assert.Equal("val444", keys["key3"].GetValue());
        }

        [Fact]
        public async void UpdateExistingIndex() {
            var idxName = "testIdx";

            var pairs = new (string, string)[] {
                ("key1", "abc"),
                ("key2", "abc"),
                ("key0", "xyz-abc"),
                ("key314", "z"),
            };
            var size = IndexPageHelpers.SizeOfEmptyPage;
            foreach (var pair in pairs)
                size += IndexPageHelpers.CalcPageItemSize(pair.Item1, pair.Item2);
            var memory = new Memory<byte>(new byte[size]);
            IndexPageHelpers.WriteHead(memory.Span, IndexPage.Kind.Page, pairs.Length);
            foreach(var pair in pairs)
                IndexPageHelpers.AppendItem(memory.Span, pair.Item1, pair.Item2);
            var page = IndexPageHelpers.CreateIndexPage(IndexesModule.GetRootId(idxName), memory);

            var oldIncId = IncrementId.NewId();
            var repo = new SimpleRepository();
            var saveResult = await repo.Save(page.CreateStamp(oldIncId), default);
            Assert.True(saveResult.IsOk, saveResult.Error.ToString());

            foreach (var pair in pairs) {
                var indexKey = IndexKeyHelpers.CreateKey(IndexesModule.GetKeyId(idxName, pair.Item1), pair.Item2);
                var saveKeyResult = await repo.Save(indexKey.CreateStamp(oldIncId), default);
                Assert.True(saveKeyResult.IsOk, saveKeyResult.Error.ToString());
            }

            var de = CreateDataEngine(repo);
            var context = new DataContext(de, new SimpleSerializer<Simple>());
            
            var record = new IndexRecord {
                new (IndexOp.Kind.Update, "key1", "val1"),
                new (IndexOp.Kind.Remove, "key0", ""),
                new (IndexOp.Kind.Update, "key4", "val444"),
            };
            var recordData = IndexRecord.Builder.EncodeRecord(record);
            var eventLog = new EventLog(IncrementId.None, IncrementId.None, recordData.Memory);
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.BuildIndex(_logger, context, idxName, eventOffset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            Assert.Equal(4, root.ItemsCount);
            Assert.Equal("abc", root.GetValueAt(0));
            Assert.Equal("key2", root.GetKeyAt(0));
            Assert.Equal("val1", root.GetValueAt(1));
            Assert.Equal("key1", root.GetKeyAt(1));
            Assert.Equal("val444", root.GetValueAt(2));
            Assert.Equal("key4", root.GetKeyAt(2));
            Assert.Equal("z", root.GetValueAt(3));
            Assert.Equal("key314", root.GetKeyAt(3));
            
            var keys = updateResult.Value.Keys;
            Assert.Equal(3, keys.Count);
            Assert.Equal("", keys["key0"].GetValue());
            Assert.Equal("val1", keys["key1"].GetValue());
            Assert.Equal("val444", keys["key4"].GetValue());
        }
    }
}