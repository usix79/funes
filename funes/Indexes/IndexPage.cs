using System;
using System.Buffers.Binary;
using System.Text;
using static Funes.Indexes.IndexPageHelpers;

namespace Funes.Indexes {
    
    public readonly struct IndexPage  {
        public enum Kind { Unknown = 0, Page = 1, Table = 2 }

        public EntityId Id { get; }
        public BinaryData Data { get; }

        public IndexPage(EntityId id, BinaryData data) => 
            (Id, Data) = (id, data);

        public ReadOnlyMemory<byte> Memory => Data.Memory;
        public ReadOnlySpan<byte> Span => Data.Memory.Span;
        public Kind PageKind => GetKind(Span);
        public int ItemsCount => GetItemsCount(Span);
        public string GetKeyAt(int idx) => Encoding.Unicode.GetString(GetKeySpan(this, idx));
        public string GetValueAt(int idx) => Encoding.Unicode.GetString(GetValueSpan(this, idx));
        public string GetValueForParent() => 
            ItemsCount > 0 
                ? PageKind == Kind.Page ? GetValueAt(0) + GetKeyAt(0) : GetValueAt(0) 
                : "";
        
        public int GetIndexOfChildPage(string key, string value) {
            var searchResult = SearchByValueParts(this, value, key);
            return searchResult >= 0 ? searchResult : ~searchResult - 1;
        }

        public int GetIndexForInsertion(string key, string value) {
            var searchResult = SearchByKeyAndValue(this, key, value);
            return searchResult < 0 ? ~searchResult : searchResult;
        }

        public int GetIndexAfter(string key, string value) {
            var searchResult = SearchByKeyAndValue(this, key, value);
            return searchResult < 0 ? ~searchResult : searchResult + 1;
        }

        public int GetIndexBefore(string key, string value) {
            var searchResult = SearchByKeyAndValue(this, key, value);
            return searchResult < 0 ? ~searchResult - 1 : searchResult - 1;
        }

        public Result<int> FindIndexOfPair(string key, string value) {
            var searchResult = SearchByKeyAndValue(this, key, value);
            return searchResult < 0
                ? Result<int>.NotFound
                : new Result<int>(searchResult);
        }
        
        public Result<int> FindIndexOfChild(EntityId childId, string childValue) {
            var searchResult = SearchByKeyAndValue(this, childId.GetName(), childValue);
            if (searchResult < 0) return Result<int>.NotFound;
            return new Result<int>(searchResult);
        }

        public BinaryStamp CreateStamp(IncrementId incId) =>
            new (Id.CreateStampKey(incId), Data);
    }
    
    public static class IndexPageHelpers {

        public static IndexPage.Kind GetKind(ReadOnlySpan<byte> span) =>
            (IndexPage.Kind)BinaryPrimitives.ReadInt32LittleEndian(span);

        public static int GetItemsCount(ReadOnlySpan<byte> span) =>
            BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));
        
        private static int GetItemOffset(ReadOnlySpan<byte> span, int idx) =>
            BinaryPrimitives.ReadInt32LittleEndian(span.Slice(8 + idx * 4));

        private static int GetCurrentSize(ReadOnlySpan<byte> span) =>
            GetItemOffset(span, GetItemsCount(span));

        public static ReadOnlySpan<byte> GetKeySpan(in IndexPage page, int itemIdx) {
            var itemOffset = GetItemOffset(page.Span, itemIdx);
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(page.Span.Slice(itemOffset));
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(page.Span.Slice(itemOffset+4));
            var keyOffset = itemOffset + 8 + valueLength * 2;
            return page.Span.Slice(keyOffset, keyLength * 2);
        }

        public static ReadOnlySpan<byte> GetValueSpan(in IndexPage page, int itemIdx) {
            var itemOffset = GetItemOffset(page.Span, itemIdx);
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(page.Span.Slice(itemOffset));
            var valueOffset = itemOffset + 8;
            return page.Span.Slice(valueOffset, valueLength * 2);
        }

        public static ReadOnlySpan<byte> GetValueKeySpan(in IndexPage page, int itemIdx) {
            var itemOffset = GetItemOffset(page.Span, itemIdx);
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(page.Span.Slice(itemOffset));
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(page.Span.Slice(itemOffset+4));
            return page.Span.Slice(itemOffset + 8, (valueLength+keyLength) * 2);
        }

        public static IndexPage CreateIndexPage(EntityId id, ReadOnlyMemory<byte> memory) =>
            new (id, new BinaryData("bin", memory.Slice(0, GetCurrentSize(memory.Span))));
        
        public static int SearchByKeyAndValue(in IndexPage page, string key, string value) {
            var start = 0;
            var end = page.ItemsCount - 1;
            while (start <= end) {
                var mid = start + ((end - start) >> 1);
                var result = Utils.Binary.CompareParts(value, key, GetValueKeySpan(page, mid)); 

                if (result == 0)
                    return mid;

                if (result > 0) {
                    start = mid + 1;
                }
                else {
                    end = mid - 1;
                }
            }
            return ~start;
        }

        public static int SearchByValueParts(in IndexPage page, string part1, string part2) {
            var start = 0;
            var end = page.ItemsCount - 1;
            while (start <= end) {
                var mid = start + ((end - start) >> 1);
                var result = Utils.Binary.CompareParts(part1, part2, GetValueSpan(page, mid)); 

                if (result == 0)
                    return mid;

                if (result > 0) {
                    start = mid + 1;
                }
                else {
                    end = mid - 1;
                }
            }
            return ~start;
        }
        
        public static int SizeOfEmptyPage => 12; // 4 bytes of kind + 4 bytes of items count + 4 bytes of real size

        private static BinaryData CreateEmptyPageData() {
            var memory = new Memory<byte>(new byte[SizeOfEmptyPage]);
            WriteHead(memory.Span, IndexPage.Kind.Page, 0);
            return new BinaryData("bin", memory);
        }

        private static BinaryData CreateEmptyRootPageData() {
            var memory = new Memory<byte>(new byte[SizeOfEmptyPage + CalcPageItemSize("", "")]);
            WriteHead(memory.Span, IndexPage.Kind.Page, 1);
            AppendItem(memory.Span, "", "");
            return new BinaryData("bin", memory);
        }

        public static BinaryData EmptyPageData = CreateEmptyPageData();

        public static BinaryData EmptyRootData = CreateEmptyRootPageData();

        public static IndexPage EmptyPage = new (EntityId.None, EmptyPageData);

        public static int CalcPageItemSize(string key, string value) =>
            4 + 8 + key.Length * 2 + value.Length * 2; // 4 bytes in indexes array + 4 bytes key len + 4 bytes value len 
        
        public static void WriteHead(Span<byte> span, IndexPage.Kind kind, int maxItemsCount) {
            var offset = 0;
            Utils.Binary.WriteInt32(span, ref offset, (int)kind);
            Utils.Binary.WriteInt32(span, ref offset, 0);
            Utils.Binary.WriteInt32(span, ref offset, 8 + 4 * (maxItemsCount+1));
        }
        public static void SetItemsCount(Span<byte> span, int count) {
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4), count);
        }

        public static void SetItemOffset(Span<byte> span, int idx, int offset) {
            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(8 + idx * 4), offset);
        }
        
        public static void AppendItem(Span<byte> span, string key, string value) {
            var offset = GetCurrentSize(span);
            
            Utils.Binary.WriteInt32(span, ref offset, value.Length);
            Utils.Binary.WriteInt32(span, ref offset, key.Length);
            Utils.Binary.WriteString(span, ref offset, value);
            Utils.Binary.WriteString(span, ref offset, key);
            
            var count = GetItemsCount(span);
            SetItemsCount(span, count + 1);
            SetItemOffset(span, count + 1, offset);
        }

        public static void CopyItems(Span<byte> span, in IndexPage page, int idxFrom, int idxTo) {
            if (idxFrom >= idxTo) return;
            
            var count = GetItemsCount(span);
            var appendOffset = GetCurrentSize(span);
            
            // copy items
            var startOffset = GetItemOffset(page.Span, idxFrom);
            var endOffset = GetItemOffset(page.Span, idxTo);
            page.Span.Slice(startOffset, endOffset - startOffset).CopyTo(span.Slice(appendOffset));
            
            // update count
            SetItemsCount(span, count + (idxTo - idxFrom));
            
            // update offsets
            for (var idx = idxFrom; idx <= idxTo; idx++) {
                var offset = GetItemOffset(page.Span, idx);
                SetItemOffset(span, count + idx - idxFrom, appendOffset + (offset - startOffset));
            }
        }

        public static (Memory<byte>, Memory<byte>) Split(Memory<byte> memory) {
            var kind = GetKind(memory.Span);
            var itemsCount = GetItemsCount(memory.Span);
            var splitItemIdx = itemsCount / 2;
            var splitItemOffset = GetItemOffset(memory.Span, splitItemIdx);
            var newPageItemsCount = (itemsCount - splitItemIdx);
            var size = SizeOfEmptyPage + 4 * newPageItemsCount + (memory.Length - splitItemOffset);
            var newMemory = new Memory<byte>(new byte[size]);
            WriteHead(newMemory.Span, kind, newPageItemsCount);
            SetItemsCount(newMemory.Span, newPageItemsCount);
            var firstItemOffset = GetItemOffset(newMemory.Span, 0);
            for (var idx = splitItemIdx; idx <= itemsCount; idx++) {
                var offset = GetItemOffset(memory.Span, idx);
                SetItemOffset(newMemory.Span, idx - splitItemIdx,  firstItemOffset + offset - splitItemOffset);
            }
            memory.Slice(splitItemOffset).CopyTo(newMemory.Slice(firstItemOffset));
            
            // just change items count in the origin page 
            SetItemsCount(memory.Span, splitItemIdx);
            return (memory.Slice(0, splitItemOffset), newMemory);
        }
    }
}