using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.ComponentModel;
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
        public Kind PageKind => GetKind(Memory);
        public int ItemsCount => GetItemsCount(Memory);
        
        public string GetKeyAt(int idx) =>
            Encoding.Unicode.GetString(GetKeyMemory(this, idx).Span);

        public string GetValueAt(int idx) =>
            Encoding.Unicode.GetString(GetValueMemory(this, idx).Span);

        public string GetValueForParent() =>
            ItemsCount > 0 ? GetValueAt(0) + GetKeyAt(0) : "";
        
        public int GetIndexForInsertion(string key, string value) {
            var searchResult = BinarySearch(this, value);
            if (searchResult < 0) return ~searchResult;
            
            var idx = searchResult;
            while (idx < ItemsCount) {
                if (CompareWithValue(this, idx, value) != 0) return idx;
                if (CompareWithKey(this, idx, key) > 0) return idx;
                idx++;
            }
            return ItemsCount;
        }

        public int GetIndexOfChildPage(string key, string value) {
            var searchResult = BinarySearch(this, key, value);
            return searchResult > 0 ? searchResult : ~searchResult - 1;
        }

        public Result<int> FindIndexOfPair(string key, string value) {
            var searchResult = BinarySearch(this, value);
            if (searchResult < 0) return Result<int>.NotFound;

            var idx = searchResult;
            while (idx < ItemsCount) {
                if (CompareWithValue(this, idx, value) != 0) return Result<int>.NotFound;
                if (CompareWithKey(this, idx, key) == 0) return new Result<int>(idx);
                idx++;
            }
            return Result<int>.NotFound;
        }
        
        public Result<int> FindIndexOfChild(EntityId childId, string childValue) {
            var searchResult = BinarySearch(this, childId.GetName(), childValue);
            if (searchResult < 0) return Result<int>.NotFound;
            return new Result<int>(searchResult);
        }

    }
    
    public static class IndexPageHelpers {

        public static IndexPage.Kind GetKind(ReadOnlyMemory<byte> memory) =>
            (IndexPage.Kind) BinaryPrimitives.ReadInt32LittleEndian(memory.Span);

        public static int GetItemsCount(ReadOnlyMemory<byte> memory) {
            return BinaryPrimitives.ReadInt32LittleEndian(memory.Slice(4).Span);
        }

        private static int GetItemOffset(ReadOnlyMemory<byte> memory, int idx) =>
            BinaryPrimitives.ReadInt32LittleEndian(memory.Slice(8 + idx * 4).Span);

        private static int GetEndOffset(ReadOnlyMemory<byte> memory) =>
            GetItemOffset(memory, GetItemsCount(memory));

        public static ReadOnlyMemory<byte> GetValueMemory(in IndexPage page, int itemIdx) {
            var itemOffset = GetItemOffset(page.Memory, itemIdx);
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(page.Memory.Slice(itemOffset).Span);
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(page.Memory.Slice(itemOffset+4).Span);
            var valueOffset = itemOffset + 8 + keyLength * 2;
            return page.Memory.Slice(valueOffset, valueLength * 2);
        }

        public static ReadOnlyMemory<byte> GetKeyMemory(in IndexPage page, int itemIdx) {
            var itemOffset = GetItemOffset(page.Memory, itemIdx);
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(page.Memory.Slice(itemOffset).Span);
            var keyOffset = itemOffset + 8;
            return page.Memory.Slice(keyOffset, keyLength * 2);
        }

        public static IndexPage CreateIndexPage(EntityId id, ReadOnlyMemory<byte> memory) {
            return new IndexPage(id, new BinaryData("bin", memory.Slice(0, GetEndOffset(memory))));
        }

        public static int CompareWithValue(in IndexPage page, int itemIdx, string strToCompare) =>
            Utils.Binary.Compare(strToCompare.AsMemory(), GetValueMemory(page, itemIdx));
        public static int CompareWithKey(in IndexPage page, int itemIdx, string key) =>
            Utils.Binary.Compare(key.AsMemory(), GetValueMemory(page, itemIdx));

        public static int CompareWithValueAndKey(in IndexPage page, int itemIdx, string value, string key) =>
            Utils.Binary.Compare(value.AsMemory(), key.AsMemory(), GetValueMemory(page, itemIdx));

        public static int BinarySearch(in IndexPage page, string value) {
            var start = 0;
            var end = page.ItemsCount - 1;
            while (start <= end) {
                var mid = start + ((end - start) >> 1);
                var result = CompareWithValue(page, mid, value);
                
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
        public static int BinarySearch(in IndexPage page, string key, string value) {
            var start = 0;
            var end = page.ItemsCount - 1;
            while (start <= end) {
                var mid = start + ((end - start) >> 1);
                var result = CompareWithValueAndKey(page, mid, value, key);

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
        
        public static int SizeOfEmptyPage => 12;

        public static BinaryData EmptyPageData =
            new ("bin", 
                WriteHead(new byte[SizeOfEmptyPage], IndexPage.Kind.Page, 0));

        public static IndexPage EmptyPage =
            new IndexPage(EntityId.None, new ("bin", 
                WriteHead(new byte[SizeOfEmptyPage], IndexPage.Kind.Page, 0)));

        public static int CalcPageItemSize(string key, string value) =>
            4 + 8 + key.Length * 2 + value.Length * 2; 
        
        public static Memory<byte> WriteHead(Memory<byte> memory, IndexPage.Kind kind, int maxItemsCount) {
            var offset = 0;
            Utils.Binary.WriteInt32(memory, ref offset, (int)kind);
            Utils.Binary.WriteInt32(memory, ref offset, 0);
            Utils.Binary.WriteInt32(memory, ref offset, 8 + 4 * (maxItemsCount+1));
            return memory;
        }
        public static void SetItemsCount(Memory<byte> memory, int count) {
            BinaryPrimitives.WriteInt32LittleEndian(memory.Slice(4).Span, count);
        }

        public static void SetItemOffset(Memory<byte> memory, int idx, int offset) {
            BinaryPrimitives.WriteInt32LittleEndian(memory.Slice(8 + idx * 4).Span, offset);
        }
        
        public static int GetAppendOffset(ReadOnlyMemory<byte> memory) {
            var count = GetItemsCount(memory);
            return BinaryPrimitives.ReadInt32LittleEndian(memory.Slice(8 + count * 4).Span);
        }
        
        public static int AppendItem(Memory<byte> memory, string key, string value) {
            var count = GetItemsCount(memory);
            var offset = GetAppendOffset(memory);
            
            Utils.Binary.WriteInt32(memory, ref offset, key.Length);
            Utils.Binary.WriteInt32(memory, ref offset, value.Length);
            Utils.Binary.WriteString(memory, ref offset, key);
            Utils.Binary.WriteString(memory, ref offset, value);

            SetItemsCount(memory, count + 1);
            BinaryPrimitives.WriteInt32LittleEndian(memory.Slice(8 + (count+1)*4).Span, offset);

            return count;
        }

        public static void CopyItems(Memory<byte> memory, in IndexPage page, int idxFrom, int idxTo) {
            var count = GetItemsCount(memory);
            var appendOffset = GetAppendOffset(memory);
            
            // copy items
            var startOffset = GetItemOffset(page.Memory, idxFrom);
            var endOffset = GetItemOffset(page.Memory, idxTo);
            page.Memory.Slice(startOffset, endOffset - startOffset).Span.CopyTo(memory.Slice(appendOffset).Span);
            
            // update count
            SetItemsCount(memory, count + (idxTo - idxFrom));
            
            // update offsets
            for (var idx = idxFrom; idx <= idxTo; idx++) {
                var offset = GetItemOffset(page.Memory, idx);
                SetItemOffset(memory, count + idx, appendOffset + (offset - startOffset));
            }
        }

        public static (Memory<byte>, Memory<byte>) Split(Memory<byte> memory) {
            var kind = GetKind(memory);
            var itemsCount = GetItemsCount(memory);
            var splitItemIdx = itemsCount / 2;
            var splitItemOffset = GetItemOffset(memory, splitItemIdx);
            var newPageItemsCount = (itemsCount - splitItemIdx);
            var size = SizeOfEmptyPage + 4 * newPageItemsCount + (memory.Length - splitItemOffset);
            var newMemory = new Memory<byte>(new byte[size]);
            WriteHead(memory, kind, itemsCount - splitItemIdx);
            var firstItemOffset = GetItemOffset(newMemory, 0);
            for (var idx = splitItemIdx; idx <= itemsCount; idx++) {
                var offset = GetItemOffset(memory, idx);
                SetItemOffset(newMemory, idx - splitItemIdx,  firstItemOffset + offset - splitItemOffset);
            }
            memory.Slice(splitItemOffset).CopyTo(newMemory.Slice(firstItemOffset));
            
            // just change items count in the origin page 
            SetItemsCount(memory, splitItemIdx);
            return (memory.Slice(0, splitItemOffset), newMemory);
        }
    }
}