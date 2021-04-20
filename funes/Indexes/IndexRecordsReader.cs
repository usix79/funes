using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Funes.Indexes {
    
    public class IndexRecordsReader: IEnumerable<IndexOp>, IEnumerator<IndexOp> {
        
        private readonly ReadOnlyMemory<byte> _data;
        private bool _initialized;
        private int _currentIdx;
        private IndexOp.Kind _opKind;
        private int _keyLength;
        private int _valLength;

        public IndexRecordsReader(ReadOnlyMemory<byte> data) {
            _data = data;
        }
        
        public bool MoveNext() {
            var idx = _initialized ? _currentIdx + 3 + _keyLength * 2 + _valLength * 2 : 0;
            if (idx >= _data.Length) return false;

            var span = _data.Span;

            _opKind = (IndexOp.Kind)span[idx];
            _keyLength = span[idx + 1];
            _valLength = span[idx + 2];
            _currentIdx = idx;
            _initialized = true;
            return true;
        }
        
        public IndexOp Current {
            get {
                var key = Encoding.Unicode.GetString(_data.Slice(_currentIdx + 3, _keyLength * 2).Span); 
                var val = Encoding.Unicode.GetString(_data.Slice(_currentIdx + 3 + _keyLength * 2, _valLength * 2).Span); 
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