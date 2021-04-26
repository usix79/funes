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
        
        public static async ValueTask<Result<int>> UploadRecord(
            DataContext context, CancellationToken ct, IncrementId incId, string indexName, IndexRecord record) {

            var recordId = GetRecordId(indexName);
            var evt = new Event(incId, IndexRecord.Builder.EncodeRecord(record).Memory);
            var result = await context.AppendEvent(recordId, evt, GetOffsetId(indexName), ct);
                
            return result.IsOk ? new Result<int>(result.Value) : new Result<int>(result.Error); 
        }

        
        private static readonly Utils.ObjectPool<StampKey[]> PremisesArr = new (() => new StampKey [1], 7);
        private static readonly Utils.ObjectPool<EntityId[]> ConclusionsArr = new (() => new EntityId [1], 7);

        public static async Task<Result<Void>> UpdateIndex(ILogger logger, DataContext context, CancellationToken ct,
            IncrementId incId, string indexName, int maxItemsOnPage) {
            
            var offsetId = GetOffsetId(indexName);
            var offsetResult = await context.Retrieve(offsetId, ct);
            if (offsetResult.IsError) return new Result<Void>(offsetResult.Error);
            var offset = new EventOffset(offsetResult.Value.Data);

            var recordId = GetRecordId(indexName);
            var eventLogResult = await context.RetrieveEventLog(recordId, offsetId, ct);
            if (eventLogResult.IsError) return new Result<Void>(eventLogResult.Error);
            var eventLog = eventLogResult.Value;

            var rebuildResult = await RebuildIndex(logger, context, indexName, offset, eventLog, maxItemsOnPage, ct);
            if (rebuildResult.IsError) return new Result<Void>(rebuildResult.Error);
            
            // try commit
            var premises = PremisesArr.Rent();
            var conclusions = ConclusionsArr.Rent();
            try {
                premises[0] = offsetResult.Value.Key;
                conclusions[0] = offsetId;
                var commitResult = await context.TryCommit(premises, conclusions, incId, ct);

                if (commitResult.IsError)
                    return new Result<Void>(commitResult.Error);
            }
            finally {
                PremisesArr.Return(premises);
                ConclusionsArr.Return(conclusions);
            }
            
            // upload
            var tasksArr = ArrayPool<ValueTask<Result<Void>>>.Shared.Rent(rebuildResult.Value.TotalItems);
            var resultsArr = ArrayPool<Result<Void>>.Shared.Rent(rebuildResult.Value.TotalItems);
            try {
                var idx = 0;
                foreach (var indexPage in rebuildResult.Value.Pages)
                    tasksArr[idx++] = context.UploadBinary(indexPage.CreateStamp(incId), ct);
                foreach (var indexKey in rebuildResult.Value.Keys)
                    tasksArr[idx++] = context.UploadBinary(indexKey.CreateStamp(incId), ct);

                var tasks = new ArraySegment<ValueTask<Result<Void>>>(tasksArr, 0, idx);
                var results = new ArraySegment<Result<Void>>(resultsArr, 0, idx);
                await Utils.Tasks.WhenAll(tasks, results, ct);

                var errorsCount = 0;
                foreach(var result in results)
                    if (result.IsError)
                        errorsCount++;

                if (errorsCount > 0) {
                    var errors = new Error[errorsCount];
                    var errorIdx = 0;
                    foreach(var result in results)
                        if (result.IsError)
                            errors[errorIdx++] = result.Error;

                    return Result<Void>.AggregateError(errors); 
                }
            }
            finally {
                ArrayPool<ValueTask<Result<Void>>>.Shared.Return(tasksArr);
                ArrayPool<Result<Void>>.Shared.Return(resultsArr);
            }

            var newOffset = offset.NextGen(eventLog.Last);
            var uploadOffsetResult = await context.UploadBinary(newOffset.CreateStamp(offsetId, incId), ct);
            if (uploadOffsetResult.IsError) return new Result<Void>(uploadOffsetResult.Error);

            var truncateResult = await context.TruncateEvents(recordId, eventLog.Last, ct);
            if (truncateResult.IsError) return new Result<Void>(truncateResult.Error);

            return new Result<Void>(Void.Value);
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
        
        private static async ValueTask<Result<IndexPage>> RetrieveIndexPage(
            EntityId pageId, IDataSource ds, CancellationToken ct) {
            var retResult = await ds.Retrieve(pageId, ct);
            if (retResult.IsError) return new Result<IndexPage>(retResult.Error);
            var data = retResult.Value.IsEmpty ? IndexPageHelpers.EmptyPageData : retResult.Value.Data;
            // TODO: validate
            return new Result<IndexPage>(new IndexPage(pageId, data));
        }
        
        private static async ValueTask<Result<IndexKey>> RetrieveIndexKey(
            string idxName, string key, IDataSource ds, CancellationToken ct) {
            var keyId = GetKeyId(idxName, key);
            var res = await ds.Retrieve(keyId, ct);
            return res.IsOk
                ? new Result<IndexKey>(new IndexKey(keyId, res.Value.Data))
                : new Result<IndexKey>(res.Error);
        }

        private static async ValueTask<Result<EventOffset>> RetrieveIndexOffset(
            string idxName, IDataSource ds, CancellationToken ct) {
            
            var offsetRes = await ds.Retrieve(GetOffsetId(idxName), ct);
            return offsetRes.IsOk
                ? new Result<EventOffset>(new EventOffset(offsetRes.Value.Data))
                : new Result<EventOffset>(offsetRes.Error);
        }
        
        private static async ValueTask<Result<List<PageOp>>> PreparePageOps(ILogger logger, IDataSource ds, 
            string idxName, EventLog log, Dictionary<EntityId, IndexPage> parents, CancellationToken ct) {

            var indexOps = ReadOps();
 
            var pageOps = new List<PageOp>(indexOps.Count);
            foreach (var op in indexOps.Values) {
                switch (op.OpKind) {
                    case IndexOp.Kind.Update:
                        var keyRes = await RetrieveIndexKey(idxName, op.Key, ds, ct);
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
                        keyRes = await RetrieveIndexKey(idxName, op.Key, ds, ct);
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
            
            Dictionary<string, IndexOp> ReadOps() {
                // for each key keep only last op
                Dictionary<string, IndexOp> ops = new();
                var reader = new IndexRecord.Reader(log.Memory);
                foreach (var op in reader) ops[op.Key] = op;
                return ops;
            }
            
            async ValueTask<Result<PageOp?>> RemoveOp(string key, string value) {
                var pageResult = await FindPage(key, value);
                if (pageResult.IsError) return new Result<PageOp?>(pageResult.Error);

                var idxResult = pageResult.Value.FindIndexOfPair(key, value);
                if (idxResult.IsError) {
                    // possible inconsistency
                    logger.FunesWarning(nameof(RemoveOp), "Value not found", $"{idxName} {key}=>{value}");
                    return new Result<PageOp?>((PageOp?)null);
                }

                var op = new PageOp(pageResult.Value, PageOp.Kind.RemoveAt, idxResult.Value, key, value);
                return new Result<PageOp?>(op);
            }

            async ValueTask<Result<PageOp>> InsertOp(string key, string value) {
                var pageResult = await FindPage(key, value);
                if (pageResult.IsError) return new Result<PageOp>(pageResult.Error);

                var idx = pageResult.Value.GetIndexForInsertion(key, value);

                var op = new PageOp(pageResult.Value, PageOp.Kind.InsertAt, idx, key, value);
                return new Result<PageOp>(op);
            }
            
            async ValueTask<Result<IndexPage>> FindPage(string key, string value) {
                var rootResult = await RetrieveIndexPage(GetRootId(idxName), ds, ct);
                if (rootResult.IsError) return new Result<IndexPage>(rootResult.Error);
                return await FindPageFrom(rootResult.Value, key, value);
            }

            async ValueTask<Result<IndexPage>> FindPageFrom(IndexPage page, string key, string value) {
                while (true) {
                    if (page.PageKind == IndexPage.Kind.Page) return new Result<IndexPage>(page);

                    var childId = page.GetKeyAt(page.GetIndexOfChildPage(key, value));
                    var childPageResult = await RetrieveIndexPage(GetPageId(idxName, childId), ds, ct);
                    if (childPageResult.IsError) return new Result<IndexPage>(childPageResult.Error);

                    parents[childPageResult.Value.Id] = page;
                    page = childPageResult.Value;
                }
            }
        }

        public readonly struct RebuildIndexResult {
            public List<IndexPage> Pages { get; init; }
            public List<IndexKey> Keys { get; init; }

            public int TotalItems => Pages.Count + Keys.Count;

            public static RebuildIndexResult CreateNew() =>
                new RebuildIndexResult {Pages = new(), Keys = new()};
        }

        public static async ValueTask<Result<RebuildIndexResult>> RebuildIndex(ILogger logger, IDataSource ds, 
            string idxName, EventOffset offset, EventLog log, int maxItemsOnPage, CancellationToken ct) {

            var parents = new Dictionary<EntityId, IndexPage>();
            var rootId = GetRootId(idxName);
            
            var pageOpsRes = await PreparePageOps(logger, ds, idxName, log, parents, ct);
            if (pageOpsRes.IsError) return new Result<RebuildIndexResult>(pageOpsRes.Error);

            var genNum = offset.Gen.ToString();
            var createdPagesCount = 0;

            var result = RebuildIndexResult.CreateNew();

            var pageOps = pageOpsRes.Value;
            while (pageOps.Count > 0) {
                pageOps.Sort();
                pageOps = UpdatePages(pageOps);
            }

            return new Result<RebuildIndexResult>(result);

            EntityId GetNewPageId() => 
                GetPageId(idxName, genNum + (- ++createdPagesCount).ToString("d4"));

            IndexPage GetParentPage(EntityId pageId) => parents![pageId];

            List<PageOp> UpdatePages(List<PageOp> aPageOps) {
                var newPageOps = new List<PageOp>();
                var (curPage, curPageIdx, curMemory) = (IndexPageHelpers.EmptyPage, 0, Memory<byte>.Empty);

                var idx = 0;
                BeginPage();
                do {
                    var op = aPageOps[idx];

                    if (op.Page.Id != curPage.Id) {
                        CompletePage();
                        BeginPage();
                    }
                    
                    switch (op.Op) {
                        case PageOp.Kind.InsertAt:
                            IndexPageHelpers.CopyItems(curMemory.Span, op.Page, curPageIdx, op.Idx);
                            IndexPageHelpers.AppendItem(curMemory.Span, op.Key, op.Value);
                            curPageIdx = op.Idx;
                            if (curPage.PageKind == IndexPage.Kind.Page)
                                result.Keys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), op.Value));
                            break;
                        case PageOp.Kind.RemoveAt:
                            IndexPageHelpers.CopyItems(curMemory.Span, op.Page, curPageIdx, op.Idx);
                            curPageIdx = op.Idx + 1; // skip item at index
                            if (curPage.PageKind == IndexPage.Kind.Page)
                                result.Keys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), ""));
                            break;
                        case PageOp.Kind.ReplaceAt:
                            IndexPageHelpers.CopyItems(curMemory.Span, op.Page, curPageIdx, op.Idx);
                            IndexPageHelpers.AppendItem(curMemory.Span, op.Key, op.Value);
                            curPageIdx = op.Idx + 1; // skip item at index
                            if (curPage.PageKind == IndexPage.Kind.Page)
                                result.Keys.Add(IndexKeyHelpers.CreateKey(GetKeyId(idxName, op.Key), op.Value));
                            break;
                        default:
                            throw new NotSupportedException();
                    }
                } while (++idx < aPageOps.Count);
                
                CompletePage();

                return newPageOps;

                void BeginPage() {
                    curPage = aPageOps[idx].Page;
                    curPageIdx = 0;
                        
                    var size = curPage.Memory.Length;
                    var count = 0;
                    while (idx + count < aPageOps.Count) {
                        var futureOp = aPageOps[idx + count];
                        if (futureOp.Page.Id != curPage.Id) break;

                        size += futureOp.Op switch {
                            PageOp.Kind.InsertAt => IndexPageHelpers.CalcPageItemSize(futureOp.Key, futureOp.Value),
                            PageOp.Kind.ReplaceAt => IndexPageHelpers.CalcPageItemSize(futureOp.Key, futureOp.Value),
                            _ => 0
                        };
                        count++;
                    }
                    curMemory = new Memory<byte>(new byte[size]);
                    IndexPageHelpers.WriteHead(curMemory.Span, IndexPage.Kind.Page, count);
                }

                void CompletePage() {
                    IndexPageHelpers.CopyItems(curMemory.Span, curPage, curPageIdx, curPage.ItemsCount);

                    var itemsCount = IndexPageHelpers.GetItemsCount(curMemory.Span);
                    if (itemsCount <= 0) {
                        if (curPage.Id == rootId) {
                            // create empty root page
                            result.Pages.Add(new IndexPage(curPage.Id, IndexPageHelpers.EmptyPageData));
                        }
                        else {
                            var parent = GetParentPage(curPage.Id);

                            var idxResult = FindIndexOfChild(parent, curPage.Id, curPage.GetValueForParent()); 
                            if (idxResult.IsOk) {
                                var newOp = new PageOp(parent, PageOp.Kind.RemoveAt, idxResult.Value, "", "");
                                newPageOps.Add(newOp);
                            }
                        }
                        
                        if (itemsCount < 0)
                            logger.FunesWarning(nameof(UpdatePages), "PageError", $"Negative items count for {curPage.Id}");
                    }
                    else if (itemsCount <= maxItemsOnPage) {
                        var page = IndexPageHelpers.CreateIndexPage(curPage.Id, curMemory); 
                        result.Pages.Add(page);

                        if (page.Id != rootId) { // check if fist items were removed
                            var currentValueForParent = curPage.GetValueForParent();
                            var newValueForParent = page.GetValueForParent();
                            if (currentValueForParent != newValueForParent) {
                                var parent = GetParentPage(curPage.Id);
                                var idxResult = FindIndexOfChild(parent, curPage.Id, currentValueForParent);
                                if (idxResult.IsOk) {
                                    var newOp = new PageOp(parent, PageOp.Kind.ReplaceAt, idxResult.Value, page.Id.GetName(), newValueForParent);
                                    newPageOps.Add(newOp);
                                }
                            }
                        }
                    }
                    else {
                        var (page1Memory, page2Memory) = IndexPageHelpers.Split(curMemory);
                        if (curPage.Id == rootId) {
                            var page1 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page1Memory); 
                            result.Pages.Add(page1);
                            var page2 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page2Memory); 
                            result.Pages.Add(page2);

                            var (key1, value1) = (page1.Id.GetName(), page1.GetValueForParent());
                            var (key2, value2) = (page2.Id.GetName(), page2.GetValueForParent());

                            var newRootSize = IndexPageHelpers.SizeOfEmptyPage
                                + IndexPageHelpers.CalcPageItemSize(key1, value1)
                                + IndexPageHelpers.CalcPageItemSize(key1, value2);
                            var newRootMemory = new Memory<byte>(new byte[newRootSize]);
                            IndexPageHelpers.WriteHead(newRootMemory.Span, IndexPage.Kind.Table, 2);
                            IndexPageHelpers.AppendItem(newRootMemory.Span, key1, value1);
                            IndexPageHelpers.AppendItem(newRootMemory.Span, key2, value2);
                            result.Pages.Add(IndexPageHelpers.CreateIndexPage(GetRootId(idxName), newRootMemory));
                        }
                        else {
                            var page1 = IndexPageHelpers.CreateIndexPage(curPage.Id, page1Memory); 
                            result.Pages.Add(page1);
                            var page2 = IndexPageHelpers.CreateIndexPage(GetNewPageId(), page2Memory); 
                            result.Pages.Add(page2);
                            
                            var (newKey, newValue) = (page2.Id.GetName(), page2.GetValueForParent());
                            var parent = GetParentPage(curPage.Id);
                            var insertIdx = parent.GetIndexForInsertion(newKey, newValue);

                            var newOp = new PageOp(parent, PageOp.Kind.InsertAt, insertIdx, newKey, newValue);
                            newPageOps.Add(newOp);

                            var currentValueForParent = curPage.GetValueForParent();
                            var newValueForParent = page1.GetValueForParent();
                            if (currentValueForParent != newValueForParent) {
                                var newOp2 = new PageOp(parent, PageOp.Kind.InsertAt, insertIdx-1, page1.Id.GetName(), newValueForParent);
                                newPageOps.Add(newOp2);
                            }
                        }
                    }
                }

                Result<int> FindIndexOfChild(in IndexPage parent, EntityId pageId, string value) {
                    var idxResult = parent.FindIndexOfChild(pageId, value);
                    if (idxResult.IsError)
                        // possible inconsistency
                        logger.FunesWarning(nameof(UpdatePages), "Child not found", 
                            $"{idxName} {parent.Id} {curPage.Id}=>{curPage.GetValueForParent()}");

                    return idxResult;
                }                
            }
        }
    }
}