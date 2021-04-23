using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Funes.Indexes {
    
    public class IndexRecord : List<IndexOp> {
        public static class Builder {
            public  static int CalcSize(IndexRecord rec) {
                var size = 0;
                foreach (var op in rec)
                    size += 12 + op.Key.Length * 2 + op.Value.Length * 2;
                return size;
            }

            public static BinaryData EncodeRecord(IndexRecord record) {
                var memory = new Memory<byte>(new byte[CalcSize(record)]);
                var span = memory.Span;
                var idx = 0;
                foreach (var op in record) {
                    Utils.Binary.WriteInt32(span, ref idx, (int)op.OpKind);
                    Utils.Binary.WriteInt32(span, ref idx, op.Key.Length);
                    Utils.Binary.WriteInt32(span, ref idx, op.Value.Length);
                    Utils.Binary.WriteString(span, ref idx, op.Key);
                    Utils.Binary.WriteString(span, ref idx, op.Value);
                }
                return new BinaryData("bin", memory);
            }
        }
        
        public class Reader: IEnumerable<IndexOp>, IEnumerator<IndexOp> {
        
            private readonly ReadOnlyMemory<byte> _memory;
            private bool _initialized;
            private int _currentIdx;
            private IndexOp.Kind _opKind;
            private int _keyLength;
            private int _valLength;

            public Reader(ReadOnlyMemory<byte> data) =>
                _memory = data;
            
            public bool MoveNext() {
                var idx = _initialized ? _currentIdx + 12 + _keyLength * 2 + _valLength * 2 : 0;
                if (idx >= _memory.Length) return false;

                var span = _memory.Span;

                _opKind = (IndexOp.Kind)BinaryPrimitives.ReadInt32LittleEndian(_memory.Slice(idx).Span);
                _keyLength = BinaryPrimitives.ReadInt32LittleEndian(_memory.Slice(idx + 4).Span);
                _valLength = BinaryPrimitives.ReadInt32LittleEndian(_memory.Slice(idx + 8).Span);
                _currentIdx = idx;
                _initialized = true;
                return true;
            }
        
            public IndexOp Current {
                get {
                    var key = Encoding.Unicode.GetString(
                        _memory.Slice(_currentIdx + 12, _keyLength * 2).Span);
                    var val = _valLength > 0
                        ? Encoding.Unicode.GetString(
                            _memory.Slice(_currentIdx + 12 + _keyLength * 2, _valLength * 2).Span)
                        : "";
                    return new IndexOp(_opKind, key, val);
                }
            }
        
            public void Reset() {
                _initialized = false;
            }

            public IEnumerator<IndexOp> GetEnumerator() => this;
        
            public void Dispose() { }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            object IEnumerator.Current => Current;
        }
    }
}