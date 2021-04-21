using System;
using System.Collections.Generic;

namespace Funes.Sets {
    
    public readonly struct SetSnapshot {

        public BinaryData Data { get; }
        
        public SetSnapshot(BinaryData data) =>
            Data = data;

        public HashSet<string> GetSet() {
            if (Data.IsEmpty) return new HashSet<string>();
            
            var idx = 0;
            var count = Utils.Binary.ReadInt32(Data.Memory, ref idx);
            var set = new HashSet<string>(count);

            while (idx < Data.Memory.Length) {
                var charsCount = Utils.Binary.ReadByte(Data.Memory, ref idx);
                var tag = charsCount > 0
                    ? Utils.Binary.ReadString(Data.Memory, ref idx, charsCount)
                    : "";
                set.Add(tag);
            }

            return set;
        }

        public static int CalcSize(HashSet<string> set) {
            var size = 4; // int count
            foreach (var tag in set) {
                size += 1 + 2 * tag.Length;
            }
            return size;
        }
        
        public static SetSnapshot FromSet(HashSet<string> set) {
            var memory = new Memory<byte>(new byte[CalcSize(set)]);
            var idx = 0;
            
            Utils.Binary.WriteInt32(memory, ref idx, set.Count);
            foreach (var tag in set) {
                memory.Span[idx++] = (byte)tag.Length;
                Utils.Binary.WriteString(memory, ref idx, tag);
            }

            return new SetSnapshot(new BinaryData("bin", memory));
        }

        public BinaryStamp CreateStamp(EntityId snapshotId, IncrementId incId) =>
            new (snapshotId.CreateStampKey(incId), Data);
    }
}