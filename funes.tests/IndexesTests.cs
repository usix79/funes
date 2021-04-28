using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Indexes;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

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
            var testSet = new IndexesTestSet();
            var eventLog = testSet.ProcessOps(new IndexOp(IndexOp.Kind.Update, "key1", "value1"));
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.UpdateIndex(_logger, ds, idxName, eventOffset, eventLog, 10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            Assert.Equal(2, root.ItemsCount);
            testSet.AssertPage(root);

            var keys = updateResult.Value.Keys;
            Assert.Single(keys);
            testSet.AssertKeys(keys);
        }
        
        [Fact]
        public async void AddingManyToNewIndex() {
            var de = CreateDataEngine();
            var ds = new DataContext(de, new SimpleSerializer<Simple>());

            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var eventLog = testSet.ProcessOps(
                new(IndexOp.Kind.Update, "key1", "val1"),
                new(IndexOp.Kind.Remove, "key2", ""),
                new(IndexOp.Kind.Update, "key3", "val333"),
                new(IndexOp.Kind.Remove, "key3", ""),
                new(IndexOp.Kind.Update, "key3", "val444"),
                new(IndexOp.Kind.Remove, "key1", "")
            );
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.UpdateIndex(_logger, ds, idxName, eventOffset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            testSet.AssertPage(root);
            
            var keys = updateResult.Value.Keys;
            Assert.Single(keys);
            testSet.AssertKeys(keys);
        }

        [Fact]
        public async void UpdateExistingIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            testSet.Init(("key1", "abc"),
                ("key2", "abc"),
                ("key0", "xyz-abc"),
                ("key314", "z")
            );

            var page = testSet.CreateIndexPage(IndexesModule.GetRootId(idxName));

            var oldIncId = IncrementId.NewId();
            var repo = new SimpleRepository();
            var saveResult = await repo.Save(page.CreateStamp(oldIncId), default);
            Assert.True(saveResult.IsOk, saveResult.Error.ToString());

            foreach (var indexKey in testSet.CreateIndexKeys(idxName)) {
                var saveKeyResult = await repo.Save(indexKey.CreateStamp(oldIncId), default);
                Assert.True(saveKeyResult.IsOk, saveKeyResult.Error.ToString());
            }

            var de = CreateDataEngine(repo);
            var context = new DataContext(de, new SimpleSerializer<Simple>());

            var eventLog = testSet.ProcessOps(
                new(IndexOp.Kind.Update, "key1", "val1"),
                new(IndexOp.Kind.Remove, "key0", ""),
                new(IndexOp.Kind.Update, "key4", "val444")
            );
            var eventOffset = new EventOffset(BinaryData.Empty);

            var updateResult = await IndexesModule.UpdateIndex(_logger, context, idxName, eventOffset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            
            var newPages = updateResult.Value.Pages;

            Assert.Single(newPages);

            var root = newPages[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(IndexPage.Kind.Page, root.PageKind);
            testSet.AssertPage(root);
            testSet.AssertKeys(updateResult.Value.Keys);
        }


        public async ValueTask PrepareTwoLayeredIndex(string idxName, DataContext context, IndexesTestSet testSet) {
            var ops = new IndexOp[15];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var eventOffset = new EventOffset(BinaryData.Empty);
            
            var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, eventOffset, eventLog,10, default);
            Assert.True(buildResult.IsOk, buildResult.Error.ToString());

            var uploadResult = await IndexesModule.Upload(context, IncrementId.NewId(), buildResult.Value, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
        } 
            
        [Fact]
        public async void TransitionToTwoLayeredIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var ops = new IndexOp[15];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var eventOffset = new EventOffset(BinaryData.Empty);
            
            var de = CreateDataEngine();
            
            var updateResult = await IndexesModule.UpdateIndex(_logger, 
                new DataContext(de, new SimpleSerializer<Simple>()), idxName, eventOffset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;
            Assert.Equal(3, newPages.Count);

            var (set1, set2) = testSet.Split();
            var root = newPages.Find(page => page.Id == IndexesModule.GetRootId(idxName));
            Assert.Equal(IndexPage.Kind.Table, root.PageKind);
            Assert.Equal(2, root.ItemsCount);
            Assert.Equal("0-0001", root.GetKeyAt(0));
            Assert.Equal("0-0002", root.GetKeyAt(1));
            Assert.Equal("", root.GetValueAt(0));
            Assert.Equal(set2.GetFirstValueKey(), root.GetValueAt(1));
            
            var page1 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0001"));
            Assert.Equal(IndexPage.Kind.Page, page1.PageKind);
            set1.AssertPage(page1);

            var page2 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0002"));
            Assert.Equal(IndexPage.Kind.Page, page2.PageKind);
            set2.AssertPage(page2);
        }

        [Fact]
        public async void UpdatesInTwoLayeredIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();

            await PrepareTwoLayeredIndex(idxName, new DataContext(de, new SimpleSerializer<Simple>()), testSet);
            var (_, snd) = testSet.Split();
            var splitValueKey = snd.GetFirstValueKey();
            
            var ops = new IndexOp[21];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var eventOffset = new EventOffset(BinaryData.Empty);
            
            var context = new DataContext(de, new SimpleSerializer<Simple>());
            var updateResult = await IndexesModule.UpdateIndex(_logger, context, idxName, eventOffset, eventLog,100, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());

            var newPages = updateResult.Value.Pages;
            Assert.Equal(2, newPages.Count);
            
            var (set1, set2) = testSet.Split(splitValueKey);
            var page1 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0001"));
            set1.AssertPage(page1);
            
            var page2 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0002"));
            set2.AssertPage(page2);
        }

        private async void CompareIndexWithTestSet(
            IDataEngine de, string idxName, IndexesTestSet testSet, int iterationsCount, bool checkIterationsCount = true) {
            var rootResult = await de.Retrieve(IndexesModule.GetRootId(idxName), default);
            Assert.True(rootResult.IsOk, rootResult.Error.ToString());
            var rootPage = new IndexPage(rootResult.Value.Eid, rootResult.Value.Data);
            if (checkIterationsCount)
                Assert.Equal(iterationsCount + 1,  rootPage.ItemsCount);

            for (var i = 0; i < rootPage.ItemsCount; i++) {
                var pageId = IndexesModule.GetPageId(idxName, rootPage.GetKeyAt(i));
                var retrievePageResult = await de.Retrieve(pageId, default);
                Assert.True(retrievePageResult.IsOk, retrievePageResult.Error.ToString());
                var page = new IndexPage(pageId, retrievePageResult.Value.Data);

                if (i < rootPage.ItemsCount - 1) {
                    var (head, tail) = testSet.Split(rootPage.GetValueAt(i+1));
                    head.AssertPage(page);
                    testSet = tail;
                }
                else {
                    var (head, tail) = testSet.Split(rootPage.GetValueAt(i));
                    tail.AssertPage(page);
                    testSet = new IndexesTestSet();
                }
            }
            
        }
        
        [Fact]
        public async void AppendToIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);

            var iterationsCount = 5;
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[11];
                for (var j = 0; j < ops.Length; j++) {
                    ops[j] = new IndexOp(IndexOp.Kind.Update, $"key-{i:d2}-{j:d2}", $"val-{i:d2}-{j:d2}");
                }
                var eventLog = testSet.ProcessOps(ops);
                var context = new DataContext(de, new SimpleSerializer<Simple>());
                var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
                Assert.True(buildResult.IsOk, buildResult.Error.ToString());
                var incId = new IncrementId($"inc-{100-i:d4}");
                var uploadResult = await IndexesModule.Upload(context, incId, buildResult.Value, default);
                Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
                offset = offset.NextGen(incId);
            }

            CompareIndexWithTestSet(de, idxName, testSet, iterationsCount);
        }

        [Fact]
        public async void PrependToIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);

            var iterationsCount = 5;
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[11];
                for (var j = 0; j < ops.Length; j++) {
                    ops[j] = new IndexOp(IndexOp.Kind.Update, $"key-{(100-i):d2}-{(100-j):d2}", $"val-{(100-i):d2}-{(100-j):d2}");
                }
                var eventLog = testSet.ProcessOps(ops);
                var context = new DataContext(de, new SimpleSerializer<Simple>());
                var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
                Assert.True(buildResult.IsOk, buildResult.Error.ToString());
                var incId = new IncrementId($"inc-{100-i:d4}");
                var uploadResult = await IndexesModule.Upload(context, incId, buildResult.Value, default);
                Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
                offset = offset.NextGen(incId);
            }

            CompareIndexWithTestSet(de, idxName, testSet, iterationsCount);
        }
        
        [Fact]
        public async void DeleteFirstItemOnPage() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);
            
            var ops = new IndexOp[10];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var context = new DataContext(de, new SimpleSerializer<Simple>());
            var updateResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            var incId = new IncrementId("100");
            var uploadResult = await IndexesModule.Upload(context, incId, updateResult.Value, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            var newPages = updateResult.Value.Pages;
            Assert.Equal(3, newPages.Count);
            
            var page2 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0002"));
            var firstKey = page2.GetKeyAt(0);
            var (_, page2Set) = testSet.Split(page2.GetValueForParent());
            var deleteOps = new[] {new IndexOp(IndexOp.Kind.Remove, firstKey, "")};
            var deleteEventLog = page2Set.ProcessOps(deleteOps);
            var deleteContext = new DataContext(de, new SimpleSerializer<Simple>());
            offset = offset.NextGen(incId);
            var updateResult2 = await IndexesModule.UpdateIndex(_logger, deleteContext, idxName, offset, deleteEventLog,10, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());
            var newPages2 = updateResult2.Value.Pages;
            Assert.Equal(2, newPages2.Count);

            var root= newPages2.Find(page => page.Id == IndexesModule.GetRootId(idxName));
            Assert.Equal(2, root.ItemsCount);
            Assert.Equal(page2Set.GetFirstValueKey(), root.GetValueAt(1));
            Assert.Equal("0-0002", root.GetKeyAt(1));
            
            var page2AfterDelete = newPages2.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0002"));
            page2Set.AssertPage(page2AfterDelete);
        }

        [Fact]
        public async void DeleteAllItemsOnPage() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);
            
            var ops = new IndexOp[10];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var context = new DataContext(de, new SimpleSerializer<Simple>());
            var updateResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            var incId = new IncrementId("100");
            var uploadResult = await IndexesModule.Upload(context, incId, updateResult.Value, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            var newPages = updateResult.Value.Pages;
            Assert.Equal(3, newPages.Count);
            
            var page2 = newPages.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0002"));
            var deleteOps = new IndexOp[page2.ItemsCount];
            for (var i = 0; i < deleteOps.Length; i++) {
                deleteOps[i] = new IndexOp(IndexOp.Kind.Remove, page2.GetKeyAt(i), "");
            }
            var (_, page2Set) = testSet.Split(page2.GetValueForParent());
            var deleteEventLog = page2Set.ProcessOps(deleteOps);
            var deleteContext = new DataContext(de, new SimpleSerializer<Simple>());
            
            offset = offset.NextGen(incId);
            var updateResult2 = await IndexesModule.UpdateIndex(_logger, deleteContext, idxName, offset, deleteEventLog,10, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());
            var newPages2 = updateResult2.Value.Pages;
            Assert.Single(newPages2);

            var root = newPages2[0];
            Assert.Equal(IndexesModule.GetRootId(idxName), root.Id);
            Assert.Equal(1, root.ItemsCount);
        }
        
        [Fact]
        public async void DeleteAllItemsInIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);
            
            var ops = new IndexOp[10];
            for (var i = 0; i < ops.Length; i++) {
                ops[i] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(5));
            }
            var eventLog = testSet.ProcessOps(ops);
            var context = new DataContext(de, new SimpleSerializer<Simple>());
            var updateResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
            Assert.True(updateResult.IsOk, updateResult.Error.ToString());
            var incId = new IncrementId("100");
            var uploadResult = await IndexesModule.Upload(context, incId, updateResult.Value, default);
            Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
            
            var newPages = updateResult.Value.Pages;
            Assert.Equal(3, newPages.Count);
            
            
            var deleteOps = new IndexOp[testSet.ItemsCount - 1];
            var idx = 0;
            foreach(var pair in testSet.GetOrderedPairs()){
                if (pair.Key == "") continue;
                deleteOps[idx++] = new IndexOp(IndexOp.Kind.Remove, pair.Key, "");
            }
        
            var deleteEventLog = testSet.ProcessOps(deleteOps);
            var deleteContext = new DataContext(de, new SimpleSerializer<Simple>());
            
            offset = offset.NextGen(incId);
            var updateResult2 = await IndexesModule.UpdateIndex(_logger, deleteContext, idxName, offset, deleteEventLog,10, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());
            var newPages2 = updateResult2.Value.Pages;
            Assert.Equal(2, newPages2.Count);

            var root= newPages2.Find(page => page.Id == IndexesModule.GetRootId(idxName));
            Assert.Equal(1, root.ItemsCount);
            Assert.Equal("", root.GetValueAt(0));
            Assert.Equal("0-0001", root.GetKeyAt(0));
            
            var page1AfterDelete = newPages2.Find(page => page.Id == IndexesModule.GetPageId(idxName, "0-0001"));
            Assert.Equal(1, page1AfterDelete.ItemsCount);
            Assert.Equal("", page1AfterDelete.GetValueAt(0));
            Assert.Equal("", page1AfterDelete.GetKeyAt(0));
        }

        [Fact]
        public async void RandomTest() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);
            HashSet<string> allKeys = new ();

            var iterationsCount = 12;
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[21];
                for (var j = 0; j < ops.Length; j++) {
                    if (allKeys.Count == 0 || RandomInt(10) > 3) {
                        var key = "key-" + RandomString(3);
                        allKeys.Add(key);
                        ops[j] = new IndexOp(IndexOp.Kind.Update, key, "val-" + RandomString(5));    
                    }
                    else {
                        var key = allKeys.ToArray()[RandomInt(allKeys.Count)];
                        allKeys.Remove(key);
                        ops[j] = new IndexOp(IndexOp.Kind.Remove, key, "");    
                    }
                }
                var eventLog = testSet.ProcessOps(ops);
                var context = new DataContext(de, new SimpleSerializer<Simple>());
                var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
                Assert.True(buildResult.IsOk, buildResult.Error.ToString());
                var incId = new IncrementId($"inc-{100-i:d4}");
                var uploadResult = await IndexesModule.Upload(context, incId, buildResult.Value, default);
                Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
                offset = offset.NextGen(incId);
            }

            CompareIndexWithTestSet(de, idxName, testSet, iterationsCount, false);
        }

    }
}