using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public static class Utils {
        
        public static class Binary {
            public static void WriteInt32(Span<byte> span, ref int offset, int val) {
                BinaryPrimitives.WriteInt32LittleEndian(span.Slice(offset, 4), val);
                offset += 4;
            }

            public static int ReadInt32(ReadOnlySpan<byte> span, ref int offset) {
                offset += 4;
                return BinaryPrimitives.ReadInt32LittleEndian(span);
            }

            public static void WriteByte(Span<byte> span, ref int offset, byte val) {
                span[offset++] = val;
            }

            public static byte ReadByte(ReadOnlySpan<byte> span, ref int offset) {
                return span[offset++];
            }

            public static void WriteString(Span<byte> span, ref int offset, string str) {
                offset += Encoding.Unicode.GetBytes(str, span.Slice(offset));
            }

            public static string ReadString(ReadOnlySpan<byte> span, ref int offset, int charsCount) {
                var len = charsCount * 2;
                var result = Encoding.Unicode.GetString(span.Slice(offset, len));
                offset += len;
                return result;
            }

            public static int Compare(ReadOnlySpan<char> stringSpan, ReadOnlySpan<byte> binSpan) =>
                MemoryMarshal.AsBytes(stringSpan).SequenceCompareTo(binSpan);

            public static int CompareParts(ReadOnlySpan<char> part1, ReadOnlySpan<char> part2, ReadOnlySpan<byte> binSpan) {
                var binPart1 = MemoryMarshal.AsBytes(part1);
                var result = binPart1.SequenceCompareTo(binSpan.Slice(0, Math.Min(binPart1.Length, binSpan.Length)));
                if (result == 0) {
                    result = MemoryMarshal.AsBytes(part2).SequenceCompareTo(binSpan.Slice(binPart1.Length));
                }

                return result;
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