using System;
using System.Threading;
using System.Threading.Tasks;

namespace Funes {
    public static class Utils {
        public static (byte, byte) CharToUtf16Bytes(char ch) => ((byte) ch, (byte) ((uint) ch >> 8));

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
    }
}