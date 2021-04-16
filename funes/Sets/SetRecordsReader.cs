using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Funes.Sets {
    
    public class SetRecordsReader : IEnumerable<SetOp>, IEnumerator<SetOp> {
        private readonly ReadOnlyMemory<byte> _data;
        private bool _initialized;
        private int _currentIdx;
        private SetOp.Kind _opKind;
        private int _tagLength;

        public SetRecordsReader(ReadOnlyMemory<byte> data) {
            _data = data;
        }

        public bool MoveNext() {
            var idx = _initialized ? _currentIdx + 2 + _tagLength * 2 : 0;
            if (idx >= _data.Length) return false;

            var span = _data.Span;

            _opKind = (SetOp.Kind)span[idx];
            _tagLength = span[idx + 1];
            _currentIdx = idx;
            _initialized = true;
            return true;
        }
        
        public SetOp Current {
            get {
                var tag = Encoding.Unicode.GetString(_data.Slice(_currentIdx + 2, _tagLength * 2).Span); 
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