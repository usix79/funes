using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Funes.Indexes {
    
    public static class IndexesModule {

        private readonly struct Dsc {
            public Dsc(EntityId recordId, EntityId offsetId, string pagesCat, string keysCat) =>
                (RecordId, OffsetId, RootId, KeysCat, PagesCat) =
                    (recordId, offsetId, new EntityId(pagesCat, "0"), keysCat, pagesCat);

            public EntityId RecordId { get; }
            public EntityId OffsetId { get; }
            public EntityId RootId { get; }
            public string KeysCat { get; }
            public string PagesCat { get; }
        }

        private static readonly ConcurrentDictionary<string, Dsc> Descriptors = new ();

        private static Dsc GetDsc(string idxName) {
            if (!Descriptors.TryGetValue(idxName, out var dsc)) {
                dsc = new Dsc(
                    new EntityId($"funes/indexes/{idxName}/records"),
                    new EntityId($"funes/indexes/{idxName}/offset"),
                    $"funes/indexes/{idxName}/pages",
                    $"funes/indexes/{idxName}/keys"
                );
                Descriptors[idxName] = dsc;
            }
            return dsc;
        }

        public static EntityId GetRecordId(string idxName) => GetDsc(idxName).RecordId;
        public static EntityId GetOffsetId(string idxName) => GetDsc(idxName).OffsetId;
        public static EntityId GetRootId(string idxName) => GetDsc(idxName).RootId;
        public static EntityId GetKeyId(string idxName, string key) =>
            new (GetDsc(idxName).KeysCat, key);
        public static EntityId GetPageId(string idxName, string pageName) =>
            new (GetDsc(idxName).PagesCat, pageName);
        public static bool IsIndexPage(EntityId eid) => 
            eid.Id.StartsWith("funes/indexes/") && eid.Id.Contains("/pages/");
        
        public static async ValueTask UploadRecords(IDataEngine de, int max,
            IncrementId incId, Dictionary<string,IndexRecord> records, List<EntityId> outputs, 
            ArraySegment<Result<string>> results, CancellationToken ct) {
            var uploadTasksArr = ArrayPool<ValueTask<Result<string>>>.Shared.Rent(records.Count);
            var uploadTasks = new ArraySegment<ValueTask<Result<string>>>(uploadTasksArr, 0, records.Count);
            try {
                var idx = 0;
                foreach (var pair in records)
                    uploadTasks[idx++] = UploadRecord(de, ct, incId, pair.Key, pair.Value, max, outputs);
                
                await Utils.Tasks.WhenAll(uploadTasks, results, ct);
            }
            finally {
                ArrayPool<ValueTask<Result<string>>>.Shared.Return(uploadTasksArr);
            }
        }
        
        // return indexName if the index needs updating of the pages
        static async ValueTask<Result<string>> UploadRecord(IDataEngine de, CancellationToken ct, 
            IncrementId incId, string indexName, IndexRecord record, int max, List<EntityId> outputs) {
            var data = RecordBuilder.EncodeRecord(record);
            var evt = new Event(incId, data.Memory);

            var recordId = GetRecordId(indexName);
            outputs.Add(recordId);
            var result = await de.AppendEvent(recordId, evt, GetOffsetId(indexName), ct);
                
            return result.IsOk
                ? new Result<string>(result.Value >= max ? indexName : "") 
                : new Result<string>(result.Error); 
        }

        public readonly struct PageOp : IComparable<PageOp> {
            public PageOp(IndexPage page, Kind op, int idx, string key, string value) =>
                (Page, Idx, Op, Key, Value) = (page, idx, op, key, value);

            public enum Kind {Unknown = 0, RemoveAt = 1, InsertAt = 2, ReplaceAt = 3};
            public IndexPage Page { get; }
            public int Idx { get; }
            public Kind Op { get; }
            public string Key { get; }
            public string Value { get; }
            
            public int CompareTo(PageOp other) {
                var pageIdComparison = Page.Id.CompareTo(other.Page.Id);
                if (pageIdComparison != 0) return pageIdComparison;
                var idxComparison = Idx.CompareTo(other.Idx);
                if (idxComparison != 0) return idxComparison;
                var opComparison = (int)Op - (int)other.Op;
                if (opComparison != 0) return opComparison;
                var valueComparison = string.Compare(Value, other.Value, StringComparison.Ordinal);
                if (valueComparison != 0) return valueComparison;
                return string.Compare(Key, other.Key, StringComparison.Ordinal);
            }
        }
        
        private static Dictionary<string, IndexOp> ReadOps(EventLog log) {
            // for each key keep only last op
            Dictionary<string, IndexOp> ops = new();
            var reader = new IndexRecordsReader(log.Memory);
            foreach (var op in reader) ops[op.Key] = op;

            return ops;
        }
        
        public static async ValueTask<Result<(List<IndexPage>, List<IndexKey>)>> UpdateIndex(
            ILogger logger, DataSource ds, string idxName, EventLog log, int maxItemsOnPage, CancellationToken ct) {

            var ops = ReadOps(log);
            var parents = new Dictionary<EntityId, EntityId>();
            
            var pageOpsRes = await PreparePageOps();
            if (pageOpsRes.IsError) return new Result<(List<IndexPage>, List<IndexKey>)>(pageOpsRes.Error);

            var offsetRes = await ds.Retrieve(GetOffsetId(idxName), ct);
            if (offsetRes.IsError) return new Result<(List<IndexPage>, List<IndexKey>)>(offsetRes.Error);
            var offset = new EventOffset(offsetRes.Value.Data);
            var genNum = offset.Gen.ToString();
            var createdPagesCount = 0;

            var newPages = new List<IndexPage>();
            var newKeys = new List<IndexKey>();

            var pageOps = pageOpsRes.Value;
            while (pageOps.Count > 0) {
                pageOps.Sort();
                var updatePagesRes = await UpdatePages(pageOps);
                if (updatePagesRes.IsError) return new Result<(List<IndexPage>, List<IndexKey>)>(updatePagesRes.Error);
                pageOps = updatePagesRes.Value;
            }

            return new Result<(List<IndexPage>, List<IndexKey>)>((newPages, newKeys));

            EntityId GetNewPageId() => 
                GetPageId(idxName, genNum + (- ++createdPagesCount).ToString("d4"));

            async ValueTask<Result<IndexPage>> GetParentPage(EntityId pageId) {
                if (!parents.TryGetValue(pageId, out var parentId))
                    return Result<IndexPage>.NotFound;

                return await RetrievePage(parentId);
            }
            
            async ValueTask<Result<List<PageOp>>> UpdatePages(List<PageOp> aPageOps) {
                var newPageOps = new List<PageOp>();
                var currentPage = IndexPageHelpers.EmptyPage;
                var currentPageIdx = 0;
                var currentMemory = Memory<byte>.Empty;
                
                var idx = 0;
                while (true) {
                    
                    if (idx == aPageOps.Count || aPageOps[idx].Page.Id != currentPage.Id) {
                        if (currentPage.Id != EntityId.None) {
                            IndexPageHelpers.CopyItems(currentMemory, currentPage, currentPageIdx, currentPage.ItemsCount);

                            var itemsCount = IndexPageHelpers.GetItemsCount(currentMemory);
                            if (itemsCount <= 0) {
                                if (currentPage.Id == GetRootId(idxName)) {
                                    // create empty root page
                                    newPages.Add(new IndexPage(currentPage.Id, IndexPageHelpers.EmptyPageData));
                                }
                                else {
                                    var parentRes = await GetParentPage(currentPage.Id);
                                    if (parentRes.IsError) return new Result<List<PageOp>>(parentRes.Error);
                                    var parent = parentRes.Value;
                                    
                                    var idxResult = parent.FindIndexOfChild(currentPage.Id, currentPage.GetValueForParent());
                                    if (idxResult.IsOk) {
                                        var newOp = new PageOp(parent, PageOp.Kind.RemoveAt, idxResult.Value, "", "");
                                        newPageOps.Add(newOp);
                                    }
                                    else {
                                        // possible inconsistency
                                        logger.FunesWarning(nameof(UpdatePages), "Child not found", 
                                            $"{idxName} {parent.Id} {currentPage.Id}=>{currentPage.GetValueForParent()}");
                                    }
                                }
                                
                                if (itemsCount < 0)
                                    logger.FunesWarning(nameof(UpdatePages), 
                                        "PageError", $"Negative items count for {currentPage.Id}");
                            }
                            else if (itemsCount <= maxItemsOnPage) {
                                var page = IndexPageHelpers.CreateIndexPage(currentPage.Id, currentMemory); 
                                newPages.Add(page);

                                if (page.Id != GetRootId(idxName)) {
                                    var currentValueForParent = currentPage.GetValueForParent();
                                    var newValueForParent = page.GetValueForParent();
                                    if (currentValueForParent != newValueForParent) {
                                        var parentRes = await GetParentPage(currentPage.Id);
                                        if (parentRes.IsError) return new Result<List<PageOp>>(parentRes.Error);

                                        var parent = parentRes.Value;
                                        var idxResult = parent.FindIndexOfChild(currentPage.Id, currentValueForParent);
                                        if (idxResult.IsOk) {
                                            var newOp = new PageOp(parent, PageOp.Kind.ReplaceAt, idxResult.Value, page.Id.GetName(), newValueForParent);
                                            newPageOps.Add(newOp);
                                        }
                                        else {
                                            // possible inconsistency
                                            logger.FunesWarning(nameof(UpdatePages), "Child not found", 
                                                $"{idxName} {parent.Id} {currentPage.Id}=>{currentPage.GetValueForParent()}");
                                        }
                                    }
                                }
                            }
                            else {
                                var (page1Memory, page2Memory) = IndexPageHelpers.Split(currentMemory);
                                if (currentPage.Id == GetRootId(idxName)) {
                                    var page1 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page1Memory); 
                                    newPages.Add(page1);
                                    var page2 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page2Memory); 
                                    newPages.Add(page2);

                                    var (key1, value1) = (page1.Id.GetName(), page1.GetValueForParent());
                                    var (key2, value2) = (page2.Id.GetName(), page2.GetValueForParent());

                                    var newRootSize = IndexPageHelpers.SizeOfEmptyPage;
                                    newRootSize += IndexPageHelpers.CalcPageItemSize(key1, value1);
                                    newRootSize += IndexPageHelpers.CalcPageItemSize(key1, value2);
                                    var newRootMemory = new Memory<byte>(new byte[newRootSize]);
                                    IndexPageHelpers.WriteHead(newRootMemory, IndexPage.Kind.Table, 2);
                                    IndexPageHelpers.AppendItem(newRootMemory, key1, value1);
                                    IndexPageHelpers.AppendItem(newRootMemory, key2, value2);
                                    newPages.Add(IndexPageHelpers.CreateIndexPage(GetRootId(idxName), newRootMemory));
                                }
                                else {
                                    var page1 = IndexPageHelpers.CreateIndexPage(currentPage.Id, page1Memory); 
                                    newPages.Add(page1);
                                    var page2 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page2Memory); 
                                    newPages.Add(page2);
                                    
                                    // TODO: add page op
                                    var parentRes = await GetParentPage(currentPage.Id);
                                    if (parentRes.IsError) return new Result<List<PageOp>>(parentRes.Error);
                                    var parent = parentRes.Value;

                                    var newKey = page2.Id.GetName();
                                    var newValue = page2.GetValueForParent();
                                    var insertIdx = parent.GetIndexForInsertion(newKey, newValue);

                                    var newOp = new PageOp(parent, PageOp.Kind.InsertAt, insertIdx, newKey, newValue);
                                    newPageOps.Add(newOp);

                                    var currentValueForParent = currentPage.GetValueForParent();
                                    var newValueForParent = page1.GetValueForParent();
                                    if (currentValueForParent != newValueForParent) {
                                        var newOp2 = new PageOp(parent, PageOp.Kind.InsertAt, insertIdx-1, page1.Id.GetName(), newValueForParent);
                                        newPageOps.Add(newOp2);
                                    }
                                }
                            }
                        }

                        if (idx == aPageOps.Count) break;

                        currentPage = aPageOps[idx].Page;
                        currentPageIdx = 0;
                        
                        var size = currentPage.Memory.Length;
                        var count = 0;
                        while (idx + count < aPageOps.Count) {
                            var futureOp = aPageOps[idx + count];
                            if (futureOp.Page.Id != currentPage.Id) break;

                            size += futureOp.Op switch {
                                PageOp.Kind.InsertAt => IndexPageHelpers.CalcPageItemSize(futureOp.Key, futureOp.Value),
                                PageOp.Kind.ReplaceAt => IndexPageHelpers.CalcPageItemSize(futureOp.Key, futureOp.Value),
                                _ => 0
                            };
                            count++;
                        }
                        currentMemory = new Memory<byte>(new byte[size]);
                        IndexPageHelpers.WriteHead(currentMemory, IndexPage.Kind.Page, count);
                    }

                    var op = aPageOps[idx];
                    
                    switch (op.Op) {
                        case PageOp.Kind.InsertAt:
                            IndexPageHelpers.CopyItems(currentMemory, op.Page, currentPageIdx, op.Idx);
                            IndexPageHelpers.AppendItem(currentMemory, op.Key, op.Value);
                            currentPageIdx = op.Idx;
                            if (currentPage.PageKind == IndexPage.Kind.Page)
                                newKeys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), op.Value));
                            break;
                        case PageOp.Kind.RemoveAt:
                            IndexPageHelpers.CopyItems(currentMemory, op.Page, currentPageIdx, op.Idx);
                            currentPageIdx = op.Idx + 1; // skip item at index
                            if (currentPage.PageKind == IndexPage.Kind.Page)
                                newKeys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), ""));
                            break;
                        case PageOp.Kind.ReplaceAt:
                            IndexPageHelpers.CopyItems(currentMemory, op.Page, currentPageIdx, op.Idx);
                            IndexPageHelpers.AppendItem(currentMemory, op.Key, op.Value);
                            currentPageIdx = op.Idx + 1; // skip item at index
                            if (currentPage.PageKind == IndexPage.Kind.Page)
                                newKeys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), op.Value));
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                    idx++;
                }
                return new Result<List<PageOp>>(newPageOps);
            }
            
            async ValueTask<Result<List<PageOp>>> PreparePageOps() {
                var pageOps = new List<PageOp>(ops.Count);
                foreach (var op in ops.Values) {
                    switch (op.OpKind) {
                        case IndexOp.Kind.Update:
                            var keyRes = await GetIndexKey(op.Key);
                            if (keyRes.IsError) return new Result<List<PageOp>>(keyRes.Error);

                            var keyValue = keyRes.Value.GetValue();
                            if (keyValue == op.Value) continue; // skip op if the index contains same value
                        
                            if (keyValue != "") {
                                var removeRes = await RemoveOp(op.Key, keyValue);
                                if (removeRes.IsError) return new Result<List<PageOp>>(removeRes.Error);
                                if (removeRes.Value.HasValue) pageOps.Add(removeRes.Value.Value);
                            }

                            var insertRes = await InsertOp(op.Key, op.Value);
                            if (insertRes.IsError) return new Result<List<PageOp>>(insertRes.Error);
                            pageOps.Add(insertRes.Value);
                            break;
                        case IndexOp.Kind.Remove:
                            keyRes = await GetIndexKey(op.Key);
                            if (keyRes.IsError) return new Result<List<PageOp>>(keyRes.Error);

                            keyValue = keyRes.Value.GetValue();
                            if (keyValue != "") {
                                var removeRes = await RemoveOp(op.Key, keyValue);
                                if (removeRes.IsError) return new Result<List<PageOp>>(removeRes.Error);
                                if (removeRes.Value.HasValue) pageOps.Add(removeRes.Value.Value);
                            }
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                }
            
                return new Result<List<PageOp>>(pageOps);
            }

            async ValueTask<Result<IndexKey>> GetIndexKey(string key) {
                var keyId = GetKeyId(idxName, key);
                var res = await ds.Retrieve(keyId, ct);
                return res.IsOk
                    ? new Result<IndexKey>(new IndexKey(keyId, res.Value.Data))
                    : new Result<IndexKey>(res.Error);
            }

            async ValueTask<Result<PageOp?>> RemoveOp(string key, string value) {
                var pageResult = await FindPage(key, value);
                if (pageResult.IsError) return new Result<PageOp?>(pageResult.Error);

                var page = pageResult.Value;

                var idxResult = page.FindIndexOfPair(key, value);
                if (idxResult.IsError) {
                    // possible inconsistency
                    logger.FunesWarning(nameof(RemoveOp), "Value not found", $"{idxName} {key}=>{value}");
                    return new Result<PageOp?>((PageOp?)null);
                }

                var op = new PageOp(page, PageOp.Kind.RemoveAt, idxResult.Value, key, value);
                return new Result<PageOp?>(op);
            }

            async ValueTask<Result<PageOp>> InsertOp(string key, string value) {
                var pageResult = await FindPage(key, value);
                if (pageResult.IsError) return new Result<PageOp>(pageResult.Error);

                var page = pageResult.Value;

                var idx = page.GetIndexForInsertion(key, value);

                var op = new PageOp(page, PageOp.Kind.InsertAt, idx, key, value);
                return new Result<PageOp>(op);
            }
            
            async ValueTask<Result<IndexPage>> FindPage(string key, string value) {
                var rootResult = await RetrievePage(GetRootId(idxName));
                if (rootResult.IsError) return new Result<IndexPage>(rootResult.Error);
                return await FindPageFrom(rootResult.Value, key, value);
            }

            async ValueTask<Result<IndexPage>> FindPageFrom(IndexPage page, string key, string value) {
                while (true) {
                    if (page.PageKind == IndexPage.Kind.Page) return new Result<IndexPage>(page);

                    var idx = page.GetIndexOfChildPage(key, value);
                    var childId = page.GetKeyAt(idx);

                    var childPageResult = await RetrievePage(GetPageId(idxName, childId));
                    if (childPageResult.IsError) return new Result<IndexPage>(childPageResult.Error);

                    parents[childPageResult.Value.Id] = page.Id;
                    page = childPageResult.Value;
                }
            }

            async ValueTask<Result<IndexPage>> RetrievePage(EntityId pageId) {
                var retResult = await ds.Retrieve(pageId, ct);
                if (retResult.IsError) return new Result<IndexPage>(retResult.Error);
                var data = retResult.Value.IsEmpty ? IndexPageHelpers.EmptyPageData : retResult.Value.Data;
                // TODO: validate
                return new Result<IndexPage>(new IndexPage(pageId, data));
            }
        }
    }
}