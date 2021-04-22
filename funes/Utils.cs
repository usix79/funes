using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public static class Utils {
        
        public static class Binary {
            public static void WriteInt32(Memory<byte> memory, ref int offset, int val) {
                var span = memory.Slice(offset, 4).Span;
                offset += 4;
                BinaryPrimitives.WriteInt32LittleEndian(span, val);
            }

            public static int ReadInt32(ReadOnlyMemory<byte> memory, ref int offset) {
                var span = memory.Slice(offset, 4).Span;
                offset += 4;
                return BinaryPrimitives.ReadInt32LittleEndian(span);
            }

            public static void WriteByte(Memory<byte> memory, ref int offset, byte val) {
                memory.Span[offset++] = val;
            }

            public static byte ReadByte(ReadOnlyMemory<byte> memory, ref int offset) {
                return memory.Span[offset++];
            }

            public static void WriteString(Memory<byte> memory, ref int offset, string str) {
                offset += Encoding.Unicode.GetBytes(str, memory.Slice(offset).Span);
            }

            public static string ReadString(ReadOnlyMemory<byte> memory, ref int offset, int charsCount) {
                var len = charsCount * 2;
                var span = memory.Slice(offset, len).Span;
                offset += len;
                return Encoding.Unicode.GetString(span);
            }

            public static int Compare(ReadOnlyMemory<char> stringMemory, ReadOnlyMemory<byte> binaryMemory) {
                var idx = 0;
                while (true) {
                    var stringIsOver = stringMemory.Length == idx;
                    var binaryIsOver = binaryMemory.Length / 2 == idx;
                    if (stringIsOver || binaryIsOver) return binaryIsOver.CompareTo(stringIsOver);

                    var charFromString = stringMemory.Span[idx];
                    var shortFromBinary = BinaryPrimitives.ReadInt16LittleEndian(binaryMemory.Slice(idx * 2).Span);
                    var charFromBinary = Convert.ToChar(shortFromBinary);

                    var result = charFromString.CompareTo(charFromBinary);

                    if (result != 0) return result;

                    idx++;
                }
            }
            public static int Compare(
                ReadOnlyMemory<char> str1, ReadOnlyMemory<char> str2, ReadOnlyMemory<byte> binMemory) {
                var idx = 0;
                while (true) {
                    var str1IsOver = str1.Length == idx;
                    var stringIsOver = str1.Length + str2.Length == idx;
                    var binaryIsOver = binMemory.Length / 2 == idx;
                    if (stringIsOver || binaryIsOver) return binaryIsOver.CompareTo(stringIsOver);

                    var charFromString = str1IsOver ? str2.Span[idx - str1.Length] : str1.Span[idx];
                    var shortFromBinary = BinaryPrimitives.ReadInt16LittleEndian(binMemory.Slice(idx * 2).Span);
                    var charFromBinary = Convert.ToChar(shortFromBinary);

                    var result = charFromString.CompareTo(charFromBinary);

                    if (result != 0) return result;

                    idx++;
                }
            }
        }
        
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

        public static class Tasks {
            public static async ValueTask WhenAll<T>(
                ArraySegment<ValueTask<Result<T>>> tasks, ArraySegment<Result<T>> results, CancellationToken ct) {

                for (var i = 0; i < tasks.Count; i++) {
                    ct.ThrowIfCancellationRequested();
                    try {
                        results[i] = await tasks[i];
                    }
                    catch (TaskCanceledException) {
                        throw;
                    }
                    catch (Exception e) {
                        results[i] = Result<T>.Exception(e);
                    }
                }
            }
            public static async ValueTask WhenAll(ArraySegment<Task> tasks, CancellationToken ct) {
                for (var i = 0; i < tasks.Count; i++) {
                    ct.ThrowIfCancellationRequested();
                    try {
                        await tasks[i];
                    }
                    catch (TaskCanceledException) {
                        throw;
                    }
                    catch (Exception) {
                    }
                }
            }
        }
    }
}