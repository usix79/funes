using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    
    /// <summary>
    /// InMemory Repository for testing purposes
    /// </summary>
    public class InMemoryRepository : IRepository {

        private record Key (MemKey MemKey, ReflectionId ReflectionId) : IComparable<Key>, IComparable {
            public int CompareTo(Key? other) {
                if (ReferenceEquals(this, other)) return 0;
                if (ReferenceEquals(null, other)) return 1;
                var memKeyComparison = MemKey.CompareTo(other.MemKey);
                if (memKeyComparison != 0) return memKeyComparison;
                return ReflectionId.CompareTo(other.ReflectionId);
            }

            public int CompareTo(object? obj) {
                if (ReferenceEquals(null, obj)) return 1;
                if (ReferenceEquals(this, obj)) return 0;
                return obj is Key other ? CompareTo(other) : throw new ArgumentException($"Object must be of type {nameof(Key)}");
            }
        }

        private readonly SortedList<Key, Mem> _list = new SortedList<Key, Mem>();
        private readonly SemaphoreSlim _lockObj = new SemaphoreSlim(1, 1); 
        
        public async Task<(Mem, ReflectionId)?> GetLatest(MemKey key) {
            await _lockObj.WaitAsync();
            try {
                var startKey = new Key(key, ReflectionId.Empty);
                
                var idx = BinarySearch(_list.Keys, startKey);
                idx = idx < 0 ? ~idx : idx;
                
                if (idx < _list.Count) {
                    var (memKey, reflectionId) = _list.Keys[idx];
                    if (memKey == key) {
                        return (_list.Values[idx], reflectionId);
                    }
                } 
                return null;
            }
            finally {
                _lockObj.Release();
            }
        } 

        public async Task<Mem?> Get(MemKey key, ReflectionId reflectionId) {
            await _lockObj.WaitAsync();
            try {
                return
                    _list.TryGetValue(new Key(key, reflectionId), out var mem)
                        ? mem
                        : null;
            }
            finally {
                _lockObj.Release();
            }
        }

        public async Task Put(Mem mem, ReflectionId reflectionId) {
            await _lockObj.WaitAsync();
            try {
                _list[new Key(mem.Key, reflectionId)] = mem;
            }
            finally {
                _lockObj.Release();
            }
        }

        public async Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1) {
            await _lockObj.WaitAsync();
            try {
                var startKey = new Key (key, before);
                var idx = BinarySearch(_list.Keys, startKey);
                idx = idx < 0 ? ~idx : idx + 1;

                var result = new List<ReflectionId>();
                for (var i = 0; i < maxCount && (idx + i < _list.Count); i++) {
                    var foundKey = _list.Keys[idx + i];
                    if (foundKey.MemKey == startKey.MemKey) {
                        result.Add(foundKey.ReflectionId);
                    }
                    else {
                        break;
                    }
                }
                return result;
            }
            finally {
                _lockObj.Release();
            }
        }
        
        #region BinarySearch
        
        /// <summary>
        /// Performs a binary search on the specified collection.
        /// source: https://stackoverflow.com/questions/967047/how-to-perform-a-binary-search-on-ilistt/2948872#2948872
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <typeparam name="TSearch">The type of the searched item.</typeparam>
        /// <param name="list">The list to be searched.</param>
        /// <param name="value">The value to search for.</param>
        /// <param name="comparer">The comparer that is used to compare the value
        /// with the list items.</param>
        /// <returns></returns>
        private static int BinarySearch<TItem, TSearch>(IList<TItem> list,
            TSearch value, Func<TSearch, TItem, int> comparer)
        {
            if (list == null)
            {
                throw new ArgumentNullException(nameof(list));
            }
            if (comparer == null)
            {
                throw new ArgumentNullException(nameof(comparer));
            }

            var lower = 0;
            var upper = list.Count - 1;

            while (lower <= upper)
            {
                var middle = lower + (upper - lower) / 2;
                var comparisonResult = comparer(value, list[middle]);
                switch (comparisonResult) {
                    case < 0:
                        upper = middle - 1;
                        break;
                    case > 0:
                        lower = middle + 1;
                        break;
                    default:
                        return middle;
                }
            }

            return ~lower;
        }

        /// <summary>
        /// Performs a binary search on the specified collection.
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="list">The list to be searched.</param>
        /// <param name="value">The value to search for.</param>
        /// <returns></returns>
        private static int BinarySearch<TItem>(IList<TItem> list, TItem value)
        {
            return BinarySearch(list, value, Comparer<TItem>.Default);
        }

        /// <summary>
        /// Performs a binary search on the specified collection.
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="list">The list to be searched.</param>
        /// <param name="value">The value to search for.</param>
        /// <param name="comparer">The comparer that is used to compare the value
        /// with the list items.</param>
        /// <returns></returns>
        private static int BinarySearch<TItem>(IList<TItem> list, TItem value,
            IComparer<TItem> comparer)
        {
            return BinarySearch(list, value, comparer.Compare);
        }
        
        #endregion
    }
}