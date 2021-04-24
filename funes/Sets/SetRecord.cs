using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Funes.Sets {
    
    public class SetRecord : List<SetOp>{

        public static class Builder {
            public static int CalcSize(SetRecord rec) {
                var size = 0;
                foreach (var op in rec)
                    size += 8 + op.Tag.Length * 2;
                return size;
            }

            public static BinaryData EncodeRecord(SetRecord setRecord) {
                var idx = 0;
                var memory = new Memory<byte>(new byte[CalcSize(setRecord)]);
                foreach (var op in setRecord) {
                    Utils.Binary.WriteInt32(memory.Span, ref idx, (int)op.OpKind);
                    Utils.Binary.WriteInt32(memory.Span, ref idx, op.Tag.Length);
                    Utils.Binary.WriteString(memory.Span, ref idx, op.Tag);
                }

                return new BinaryData("bin", memory);
            }
        }
        
        public class Reader : IEnumerable<SetOp>, IEnumerator<SetOp> {
            private readonly ReadOnlyMemory<byte> _data;
            private bool _initialized;
            private int _currentIdx;
            private SetOp.Kind _opKind;
            private int _tagLength;

            public Reader(ReadOnlyMemory<byte> data) {
                _data = data;
            }

            public bool MoveNext() {
                var idx = _initialized ? _currentIdx + 8 + _tagLength * 2 : 0;
                if (idx >= _data.Length) return false;

                _opKind = (SetOp.Kind)BinaryPrimitives.ReadInt32LittleEndian(_data.Span.Slice(idx));
                _tagLength = BinaryPrimitives.ReadInt32LittleEndian(_data.Span.Slice(idx + 4));
                _currentIdx = idx;
                _initialized = true;
                return true;
            }
        
            public SetOp Current {
                get {
                    var tag = Encoding.Unicode.GetString(_data.Span.Slice(_currentIdx + 8, _tagLength * 2)); 
                    return new SetOp(_opKind, tag);
                }
            }

            public void Reset() {
                _initialized = false;
            }

            object IEnumerator.Current => Current;

            public void Dispose() { }
            public IEnumerator<SetOp> GetEnumerator() => this;

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }
    }
}