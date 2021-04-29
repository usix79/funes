using System;
using System.Collections.Generic;
using System.Linq;
using Funes.Indexes;
using Xunit;

namespace Funes.Tests {
    
    public class IndexesTestSet {

        private readonly Dictionary<string, string> _data = new();

        public IndexesTestSet() {
            _data[""] = "";
        }

        public IndexesTestSet(IEnumerable<(string, string)> pairs) {
            Init(pairs.ToArray());
        }

        public KeyValuePair<string,string>[] GetOrderedPairs() => 
            _data.OrderBy(pair => pair.Value + pair.Key).ToArray();

        public void Init(params (string, string)[] pairs) {
            foreach (var pair in pairs)
                _data[pair.Item1] = pair.Item2;
        }

        public IndexPage CreateIndexPage(EntityId entityId) {
            var pairs = GetOrderedPairs();
            var size = IndexPageHelpers.SizeOfEmptyPage;
            foreach (var pair in pairs)
                size += IndexPageHelpers.CalcPageItemSize(pair.Key, pair.Value);
            var memory = new Memory<byte>(new byte[size]);
            IndexPageHelpers.WriteHead(memory.Span, IndexPage.Kind.Page, pairs.Length);
            foreach(var pair in pairs)
                IndexPageHelpers.AppendItem(memory.Span, pair.Key, pair.Value);
            return IndexPageHelpers.CreateIndexPage(entityId, memory);
        }

        public IndexKey[] CreateIndexKeys(string idxName) =>
            _data
                .Select(pair => IndexKeyHelpers.CreateKey(IndexesModule.GetKeyId(idxName, pair.Key), pair.Value))
                .ToArray();

        public EventLog ProcessOps(params IndexOp[] ops) {
            var record = new IndexRecord ();
            foreach(var op in ops)
                record.Add(op);
            
            Update(record);

            var recordData = IndexRecord.Builder.EncodeRecord(record);
            var eventLog = new EventLog(IncrementId.None, IncrementId.None, recordData.Memory);
            return eventLog;
        }
        
        public void Update(IndexRecord record) {
            foreach (var op in record) {
                switch (op.OpKind) {
                    case IndexOp.Kind.Remove:
                        _data.Remove(op.Key);
                        break;
                    case IndexOp.Kind.Update:
                        _data[op.Key] = op.Value;
                        break;
                }
            }
        }

        public void AssertPage(IndexPage page) {
            Assert.Equal(_data.Count, page.ItemsCount);
            var pairs = GetOrderedPairs();
            for (var i = 0; i < pairs.Length; i++) {
                Assert.Equal(pairs[i].Value, page.GetValueAt(i));
                Assert.Equal(pairs[i].Key, page.GetKeyAt(i));
            }
        }

        public void AssertKeys(Dictionary<string, IndexKey> keys) {
            foreach (var pair in keys) {
                var keyValue = pair.Value.GetValue();
                if (keyValue != "") {
                    Assert.Equal(_data[pair.Key], pair.Value.GetValue());
                }
                else {
                    Assert.False(_data.ContainsKey(pair.Key));
                }
            }
        }

        public (IndexesTestSet, IndexesTestSet) Split(string splitItem = "") {
            var pairs = GetOrderedPairs();
            var itemsInFirstPage = splitItem == ""
                ? pairs.Length / 2
                : Array.FindIndex(pairs,pair => pair.Value + pair.Key == splitItem);
            var set1 = new IndexesTestSet(pairs.Take(itemsInFirstPage).Select(pair => (pair.Key,pair.Value)));
            var set2 = new IndexesTestSet(pairs.Skip(itemsInFirstPage).Select(pair => (pair.Key,pair.Value)));
            return (set1, set2);
        }

        public int ItemsCount => _data.Count;

        public string GetFirstValueKey() {
            var pair = GetOrderedPairs().First();
            return pair.Value + pair.Key;
        }

        public string GetKeyAt(int idx) => 
            GetOrderedPairs().Select(pair => pair.Key).ElementAt(idx);

        public string GetValueAt(int idx) => 
            GetOrderedPairs().Select(pair => pair.Value).ElementAt(idx);

        public (KeyValuePair<string,string>[], bool) Select(string valueFrom, string? valueTo, string afterKey, int maxCount) {
            var from = valueFrom + afterKey;

            var pairs = GetOrderedPairs()
                .SkipWhile(pair => string.CompareOrdinal(pair.Value + pair.Key, from) <= 0)
                .Where(pair => valueTo == null || string.CompareOrdinal(pair.Value, valueTo) <= 0)
                .ToArray();

            return (pairs.Take(maxCount).ToArray(), pairs.Length > maxCount);
        }
    }
}