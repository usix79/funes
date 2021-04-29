using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Funes.Impl;
using Funes.Indexes;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public class IndexesTests {
        private readonly ILogger _logger;

        private IDataEngine CreateDataEngine(IRepository? repository = null) {
            var repo = repository ?? new SimpleRepository();
            var cache = new SimpleCache();
            var tre = new SimpleTransactionEngine();
            return new StatelessDataEngine(repo, cache, tre, _logger);
        }
        
        public IndexesTests(ITestOutputHelper testOutputHelper) {
            _logger = XUnitLogger.CreateLogger(testOutputHelper);
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

        private async ValueTask CompareIndexWithTestSet(
            IDataEngine de, string idxName, IndexesTestSet testSet) {
            var rootResult = await de.Retrieve(IndexesModule.GetRootId(idxName), default);
            Assert.True(rootResult.IsOk, rootResult.Error.ToString());
            var rootPage = new IndexPage(rootResult.Value.Eid, rootResult.Value.Data);

            await ComparePageWithTestSet(de, idxName, rootPage, testSet);
        }

        private async ValueTask ComparePageWithTestSet(IDataEngine de, string idxName, IndexPage page, IndexesTestSet testSet) {
            if (page.PageKind == IndexPage.Kind.Page) {
                testSet.AssertPage(page);
            }
            else {
                for (var i = 0; i < page.ItemsCount; i++) {
                    var pageId = IndexesModule.GetPageId(idxName, page.GetKeyAt(i));
                    var retrievePageResult = await de.Retrieve(pageId, default);
                    Assert.True(retrievePageResult.IsOk, retrievePageResult.Error.ToString());
                    var childPage = new IndexPage(pageId, retrievePageResult.Value.Data);

                    if (i < page.ItemsCount - 1) {
                        var (head, tail) = testSet.Split(page.GetValueAt(i+1));
                        await ComparePageWithTestSet(de, idxName, childPage, head);
                        testSet = tail;
                    }
                    else {
                        await ComparePageWithTestSet(de, idxName, childPage, testSet);
                        testSet = new IndexesTestSet();
                    }
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

            await CompareIndexWithTestSet(de, idxName, testSet);
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

            await CompareIndexWithTestSet(de, idxName, testSet);
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
        public async void DeleteAllItems() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);
            
            var iterationsCount = 11;
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[11];
                for (var j = 0; j < ops.Length; j++) {
                    ops[j] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(2), "val-" + RandomString(5));    
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

            var deleteOps = new IndexOp[testSet.ItemsCount - 1];
            var idx = 0;
            var pairs = testSet.GetOrderedPairs();
            foreach(var pair in pairs){
                if (pair.Key == "") continue;
                deleteOps[idx++] = new IndexOp(IndexOp.Kind.Remove, pair.Key, "");
            }
        
            var deleteEventLog = testSet.ProcessOps(deleteOps);
            var deleteContext = new DataContext(de, new SimpleSerializer<Simple>());
            
            offset = offset.NextGen(new IncrementId("inc-0000"));
            var updateResult2 = await IndexesModule.UpdateIndex(_logger, deleteContext, idxName, offset, deleteEventLog,10, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());
            var newPages2 = updateResult2.Value.Pages;
            Assert.Equal(3, newPages2.Count);

            var root= newPages2.Find(page => page.Id == IndexesModule.GetRootId(idxName));
            Assert.Equal(1, root.ItemsCount);
            Assert.Equal("", root.GetValueAt(0));
            
            var tableAfterDelete = newPages2.Find(page => page.Id == IndexesModule.GetPageId(idxName, root.GetKeyAt(0)));
            Assert.Equal(1, tableAfterDelete.ItemsCount);
            Assert.Equal("", tableAfterDelete.GetValueAt(0));

            var pageAfterDelete = newPages2.Find(page => page.Id == IndexesModule.GetPageId(idxName, tableAfterDelete.GetKeyAt(0)));
            Assert.Equal(1, pageAfterDelete.ItemsCount);
            Assert.Equal("", pageAfterDelete.GetValueAt(0));
            Assert.Equal("", pageAfterDelete.GetKeyAt(0));

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

            await CompareIndexWithTestSet(de, idxName, testSet);
        }

        [Fact]
        public async void ThreeLayeredIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);

            var iterationsCount = 11;
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

            var retrieveRootResult = await de.Retrieve(IndexesModule.GetRootId(idxName), default);
            Assert.True(retrieveRootResult.IsOk, retrieveRootResult.Error.ToString());
            var root = new IndexPage(retrieveRootResult.Value.Eid, retrieveRootResult.Value.Data);
            
            Assert.Equal(IndexPage.Kind.Table, root.PageKind);
            Assert.Equal(2, root.ItemsCount);
            
            await CompareIndexWithTestSet(de, idxName, testSet);
        }

        [Fact]
        public async void DeleteTableInThreeLayeredIndex() {
            var idxName = "testIdx";
            var testSet = new IndexesTestSet();
            var de = CreateDataEngine();
            var offset = new EventOffset(BinaryData.Empty);

            var iterationsCount = 11;
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[11];
                for (var j = 0; j < ops.Length; j++) {
                    ops[j] = new IndexOp(IndexOp.Kind.Update, $"key-{i:d2}-{j:d2}", $"val-{i:d2}-{j:d2}");
                }
                var eventLog = testSet.ProcessOps(ops);
                var context = new DataContext(de, new SimpleSerializer<Simple>());
                var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
                Assert.True(buildResult.IsOk, buildResult.Error.ToString());
                var incId = new IncrementId("100");
                var uploadResult = await IndexesModule.Upload(context, incId, buildResult.Value, default);
                Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
                offset = offset.NextGen(incId);
            }

            var retrieveRootResult = await de.Retrieve(IndexesModule.GetRootId(idxName), default);
            Assert.True(retrieveRootResult.IsOk, retrieveRootResult.Error.ToString());
            var root = new IndexPage(retrieveRootResult.Value.Eid, retrieveRootResult.Value.Data);
            
            Assert.Equal(IndexPage.Kind.Table, root.PageKind);
            Assert.Equal(2, root.ItemsCount);

            var (set1, set2) = testSet.Split(root.GetValueAt(1));
            var deleteOps = new IndexOp[set2.ItemsCount];
            var pairs = set2.GetOrderedPairs();
            for (var i = 0; i < pairs.Length; i++)
                deleteOps[i] = new IndexOp(IndexOp.Kind.Remove, pairs[i].Key, "");
            
            var deleteEventLog = testSet.ProcessOps(deleteOps);
            var deleteContext = new DataContext(de, new SimpleSerializer<Simple>());
            
            var incId2 = new IncrementId("099");
            offset = offset.NextGen(incId2);
            var updateResult2 = await IndexesModule.UpdateIndex(_logger, deleteContext, idxName, offset, deleteEventLog,10, default);
            Assert.True(updateResult2.IsOk, updateResult2.Error.ToString());
            var uploadResult2 = await IndexesModule.Upload(deleteContext, incId2, updateResult2.Value, default);
            Assert.True(uploadResult2.IsOk, uploadResult2.Error.ToString());
            
            var newPages2 = updateResult2.Value.Pages;
            Assert.Single(newPages2);
            root = newPages2[0];
            Assert.Equal(IndexPage.Kind.Table, root.PageKind);
            Assert.Equal(1, root.ItemsCount);

            await CompareIndexWithTestSet(de, idxName, set1);
        }

        private async ValueTask<IndexesTestSet> GenerateRandomIndex(IDataEngine de, string idxName, int iterationsCount) {
            var testSet = new IndexesTestSet();
            var offset = new EventOffset(BinaryData.Empty);
            for (var i = 0; i < iterationsCount; i++) {
                var ops = new IndexOp[11];
                for (var j = 0; j < ops.Length; j++) {
                    if (RandomInt(10) > 2) {
                        ops[j] = new IndexOp(IndexOp.Kind.Update, "key-" + RandomString(3), "val-" + RandomString(2));    
                    }
                    else {
                        ops[j] = new IndexOp(IndexOp.Kind.Remove, testSet.GetRandomKey(RandomInt(testSet.ItemsCount)), "");    
                    }
                }
                var eventLog = testSet.ProcessOps(ops);
                var context = new DataContext(de, new SimpleSerializer<Simple>());
                var buildResult = await IndexesModule.UpdateIndex(_logger, context, idxName, offset, eventLog,10, default);
                Assert.True(buildResult.IsOk, buildResult.Error.ToString());
                var incId = new IncrementId($"inc-{1000-i:d4}");
                var uploadResult = await IndexesModule.Upload(context, incId, buildResult.Value, default);
                Assert.True(uploadResult.IsOk, uploadResult.Error.ToString());
                offset = offset.NextGen(incId);
            }

            return testSet;
        }

        private async void AssertSelect(IDataEngine de, string idxName, IndexesTestSet testSet, 
            string valueFrom, string? valueTo, string afterKey, int maxCount, IndexRecord? record = null) {

            var (expectedPairs, expectedHasMore) = testSet.Select(valueFrom, valueTo, afterKey, maxCount);

            if (record != null) {
                var newTestSet = new IndexesTestSet(expectedPairs.Select(pair => (pair.Key, pair.Value)));
                var eventLog = newTestSet.ProcessOps(record.ToArray());
                var recordId = IndexesModule.GetRecordId(idxName);
                var offsetId = IndexesModule.GetOffsetId(idxName);
                var incId = new IncrementId("0");
                var appendResult = await de.AppendEvent(recordId, new Event(incId, eventLog.Memory), offsetId, default);
                Assert.True(appendResult.IsOk, appendResult.Error.ToString());

                expectedPairs = newTestSet.Select(valueFrom, valueTo, afterKey, newTestSet.ItemsCount + 1).Item1;
            }

            var selectContext = new DataContext(de, new SimpleSerializer<Simple>());
            var selectResult = await IndexesModule.Select(selectContext, default, idxName, valueFrom, valueTo, afterKey, maxCount);
            Assert.True(selectResult.IsOk, selectResult.Error.ToString());

            var equals = AssertSequencesEqual(expectedPairs, selectResult.Value.Pairs);
            if (!equals) {
                _logger.LogInformation(PrepareMessage());
                Assert.Equal(expectedPairs, selectResult.Value.Pairs);
            }

            if (expectedHasMore != selectResult.Value.HasMore) {
                _logger.LogInformation(PrepareMessage());
                Assert.Equal(expectedHasMore, selectResult.Value.HasMore);
            }
            
            string PrepareMessage() {
                var txt = new StringBuilder();
                txt.AppendLine($"from '{valueFrom}' afterKey '{afterKey}' to '{valueTo}' {maxCount}");
                txt.Append("Expected:");
                foreach (var pair in expectedPairs)
                    txt.Append($"({pair.Key},{pair.Value}) ");
                if (expectedHasMore)
                    txt.Append(" has more");
                txt.AppendLine();
                txt.Append("Actual:  ");
                foreach (var pair in selectResult.Value.Pairs)
                    txt.Append($"({pair.Key},{pair.Value}) ");
                if (selectResult.Value.HasMore)
                    txt.Append(" has more");
                txt.AppendLine();
                return txt.ToString();
            }
        }
        
        [Fact]
        public async void SelectWithValueTo() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;
            var valueTo = pairs[5].Value;
            
            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 5);
        }

        [Fact]
        public async void SelectWithMaxCount() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;

            AssertSelect(de, idxName, testSet,  valueFrom, null, "", 5);
        }

        [Fact]
        public async void SelectWithAfterKey() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;
            var afterKey = pairs[3].Key;

            AssertSelect(de, idxName, testSet,  valueFrom, null, afterKey, 5);
        }

        [Fact]
        public async void SelectEmptyRangeInTheMiddleItem() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value + "-1";
            var valueTo = pairs[3].Value + "-2";

            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 5);
        }

        [Fact]
        public async void SelectLastItem() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[^1].Value;

            AssertSelect(de, idxName, testSet,  valueFrom, null, "", 5);
        }

        [Fact]
        public async void SelectAfterLastItem() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[^1].Value;
            var afterKey = pairs[^1].Key;

            AssertSelect(de, idxName, testSet,  valueFrom, null, afterKey, 5);
        }

        [Fact]
        public async void SelectFromGreaterThanLastItem() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[^1].Value + "!";

            AssertSelect(de, idxName, testSet,  valueFrom, null, "", 5);
        }

        [Fact]
        public async void SelectFromBeginning() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();

            AssertSelect(de, idxName, testSet,  "", null, "", 5);
        }

        [Fact]
        public async void SelectFromFirstItem() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[1].Value;

            AssertSelect(de, idxName, testSet,  valueFrom, null, "", 5);
        }

        [Fact]
        public async void SelectWhenEventLogHasNewItemsInRange() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;
            var valueTo = pairs[5].Value;

            var record = new IndexRecord() {
                new(IndexOp.Kind.Update, "key1", pairs[4].Value + "-1"),
                new(IndexOp.Kind.Update, "key1", pairs[4].Value + "-2"),
            };

            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 5, record);
        }

        [Fact]
        public async void SelectWhenEventLogHasRemovedAnItemInRange() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 3);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;
            var valueTo = pairs[5].Value;

            var record = new IndexRecord() {
                new(IndexOp.Kind.Remove, pairs[4].Key, ""),
                new(IndexOp.Kind.Update, pairs[5].Key, pairs[^1].Value),
            };

            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 5, record);
        }

        [Fact]
        public async void SelectWhenOnlyEventLogExistForAnIndex() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 0);
            
            var record = new IndexRecord() {
                new(IndexOp.Kind.Update, "key1", "value1"),
                new(IndexOp.Kind.Update, "key2", "value2"),
                new(IndexOp.Kind.Update, "key3", "value3"),
                new(IndexOp.Kind.Update, "key4", "value4"),
                new(IndexOp.Kind.Update, "key5", "value5"),
            };

            AssertSelect(de, idxName, testSet,  record[1].Value, record[3].Value, "", 5, record);
        }

        [Fact]
        public async void SelectWhenEventLogHasNewItemsNotInRange() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 2);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[3].Value;
            var valueTo = pairs[5].Value;

            var record = new IndexRecord() {
                new(IndexOp.Kind.Update, "key1", pairs[8].Value + "-1"),
                new(IndexOp.Kind.Update, "key2", pairs[9].Value + "-2"),
            };

            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 5, record);
        }

        
        [Fact]
        public async void SelectInThreeLayeredIndex() {
            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 100);

            var pairs = testSet.GetOrderedPairs();
            var valueFrom = pairs[123].Value;
            var valueTo = pairs[321].Value;
            
            AssertSelect(de, idxName, testSet,  valueFrom, valueTo, "", 1000);
        }

        [Theory, Repeat(42)]
        public async void RandomSelect() {

            var idxName = "testIdx";
            var de = CreateDataEngine();

            var testSet = await GenerateRandomIndex(de, idxName, 100);
            var pairs = testSet.GetOrderedPairs();

            for (var i = 0; i < 100; i++) {
                var valueFrom = pairs[RandomInt(pairs.Length)].Value;
                var valueTo = RandomInt(10) < 5 ? null : pairs[RandomInt(pairs.Length)].Value;
                var afterKey = RandomInt(10) < 5 ? null : pairs[RandomInt(pairs.Length)].Key;
                var count = RandomInt(pairs.Length);

                AssertSelect(de, idxName, testSet,  valueFrom, valueTo, afterKey, count);
            }
        }
    }
}