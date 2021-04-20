using System;
using System.Buffers.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public static class Utils {

        public static BinaryData EncodeOffset(IncrementId incId) =>
            new ("utf16", Encoding.Unicode.GetBytes(incId.Id));
        
        public static Result<IncrementId> DecodeOffset(BinaryData data) {
            try {
                var id = Encoding.Unicode.GetString(data.Memory.Span);
                return new Result<IncrementId>(new IncrementId(id));
            }
            catch (Exception x) {
                return Result<IncrementId>.Exception(x);
            }
        }

        public static class Binary {
            public static void WriteInt32(Memory<byte> memory, ref int idx, int val) {
                var span = memory.Slice(idx, 4).Span;
                idx += 4;
                BinaryPrimitives.WriteInt32LittleEndian(span, val);
            }

            public static int ReadInt32(ReadOnlyMemory<byte> memory, ref int idx) {
                var span = memory.Slice(idx, 4).Span;
                idx += 4;
                return BinaryPrimitives.ReadInt32LittleEndian(span);
            }

            public static void WriteByte(Memory<byte> memory, ref int idx, byte val) {
                memory.Span[idx++] = val;
            }

            public static byte ReadByte(ReadOnlyMemory<byte> memory, ref int idx) {
                return memory.Span[idx++];
            }

            public static void WriteString(Memory<byte> memory, ref int idx, string str) {
                idx += Encoding.Unicode.GetBytes(str, memory.Slice(idx).Span);
            }

            public static string ReadString(ReadOnlyMemory<byte> memory, ref int idx, int charsCount) {
                var len = charsCount * 2;
                var span = memory.Slice(idx, len).Span;
                idx += len;
                return Encoding.Unicode.GetString(span);
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