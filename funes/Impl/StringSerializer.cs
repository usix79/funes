using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Impl {
    
    public class StringSerializer : ISerializer {

        public static readonly StringSerializer Instance = new StringSerializer();
        public ValueTask<Result<string>> Encode(Stream output, EntityId eid, object content, CancellationToken ct) {
            var str = (string) content;
            foreach (var ch in str) {
                var loByte = (byte) ch;
                var hiByte = (byte)((uint) ch >> 8);
                output.WriteByte(loByte);
                output.WriteByte(hiByte); // little endian
            }
            return ValueTask.FromResult(new Result<string>("utf16"));
        }

        public ValueTask<Result<object>> Decode(Stream input, EntityId eid, string encoding, CancellationToken ct) {
            var arr = ArrayPool<byte>.Shared.Rent(256);

            var offset = 0;
            while (true) {
                var b = input.ReadByte();
                if (b == -1) break;

                if (offset == arr.Length) {
                    var oldArr = arr;
                    arr = ArrayPool<byte>.Shared.Rent(arr.Length * 2);
                    oldArr.CopyTo(arr, 0);
                    ArrayPool<byte>.Shared.Return(oldArr);
                }

                arr[offset++] = (byte) b;
            }

            var result = Encoding.Unicode.GetString(arr, 0, offset);
            ArrayPool<byte>.Shared.Return(arr);

            return ValueTask.FromResult(new Result<object>(result));
        }
    }
}