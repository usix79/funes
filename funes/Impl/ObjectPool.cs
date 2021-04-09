using System;
using System.Threading;

namespace Funes.Impl {
    
#nullable disable
    public class ObjectPool<T> where T : class {
        private T _firstItem;
        private readonly T[] _items;
        private readonly Func<T> _generator;

        public ObjectPool(Func<T> generator, int size) {
            _generator = generator ?? throw new ArgumentNullException(nameof(generator));
            _items = new T[size - 1];
        }

        public T Rent() {
            var inst = _firstItem;
            if (inst == null || inst != Interlocked.CompareExchange
                (ref _firstItem, null, inst)) {
                inst = RentSlow();
            }

            return inst;
        }

        public void Return(T item) {
            if (_firstItem == null) {
                _firstItem = item;
            }
            else {
                ReturnSlow(item);
            }
        }

        private T RentSlow() {
            for (var i = 0; i < _items.Length; i++) {
                var inst = _items[i];
                if (inst != null) {
                    if (inst == Interlocked.CompareExchange(ref _items[i], null, inst)) {
                        return inst;
                    }
                }
            }

            return _generator();
        }

        private void ReturnSlow(T obj) {
            for (var i = 0; i < _items.Length; i++) {
                if (_items[i] == null) {
                    _items[i] = obj;
                    break;
                }
            }
        }
    }
#nullable restore
}