using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root;

        public Task<Result<Void>> Save(BinaryStamp stamp, CancellationToken ct) {
            return Write(stamp.Key,  stamp.Data.Memory, stamp.Data.Encoding, ct);
        }
        
        private async Task<Result<Void>> Write(StampKey key, 
                        ReadOnlyMemory<byte> data, string encoding, CancellationToken ct) {
            try {
                Directory.CreateDirectory(GetMemPath(key.EntId));
                var fileName = GetMemFileName(key, encoding);
                await using FileStream fs = File.OpenWrite(fileName);
                await fs.WriteAsync(data, ct);
                return new Result<Void>(Void.Value);
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) { return Result<Void>.Exception(e); }
        }
        
        public async Task<Result<BinaryStamp>> Load(StampKey key, CancellationToken ct) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    foreach (var fileName in Directory.GetFiles(memDirectory, GetMemFileMask(key))) {
                        ct.ThrowIfCancellationRequested();
                        var (_, encoding) = ParseFileName(fileName);
                        await using MemoryStream stream = new();
                        await File.OpenRead(fileName).CopyToAsync(stream, ct);
                        if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                        return new Result<BinaryStamp>(new BinaryStamp(key, new BinaryData(encoding, buffer)));
                    }
                }
                return Result<BinaryStamp>.NotFound;
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception x) { return Result<BinaryStamp>.Exception(x); }
        }

        public async ValueTask<Result<ReadOnlyMemory<byte>>> LoadBinary(StampKey key, CancellationToken ct) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    var fileName = GetEventFileName(key);
                    var fullFileName = Path.Combine(memDirectory, fileName);
                    if (File.Exists(fullFileName)) {
                        var data = await File.ReadAllBytesAsync(fullFileName, ct);
                        return new Result<ReadOnlyMemory<byte>>(data);
                    }
                }
                return Result<ReadOnlyMemory<byte>>.NotFound;
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception x) { return Result<ReadOnlyMemory<byte>>.Exception(x); }
        }
        
        public Task<Result<IncrementId[]>> HistoryBefore(EntityId eid,
            IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();
            
            try {
                var path = GetMemPath(eid);

                var incIds =
                    Directory.Exists(path)
                        ? Directory.GetFiles(path)
                            .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                            .Where(before.IsNewerThan)
                            .OrderBy(x => x)
                            .Take(maxCount)
                            .ToArray()
                        : Array.Empty<IncrementId>();

                return Task.FromResult(new Result<IncrementId[]>(incIds));
            }
            catch (Exception e) {
                return Task.FromResult(Result<IncrementId[]>.Exception(e));
            }
        }

        public Task<Result<IncrementId[]>> HistoryAfter(EntityId eid,
            IncrementId after, CancellationToken ct = default) {

            ct.ThrowIfCancellationRequested();

            try {
                var path = GetMemPath(eid);

                var incIds =
                    Directory.Exists(path)
                        ? Directory.GetFiles(path)
                            .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                            .Where(after.IsOlderThan)
                            .OrderByDescending(x => x)
                            .ToArray()
                        : Array.Empty<IncrementId>();

                return Task.FromResult(new Result<IncrementId[]>(incIds));
            }
            catch (Exception e) {
                return Task.FromResult(Result<IncrementId[]>.Exception(e));
            }
        }

        public Task<Result<string[]>> List(string category, string after = "", int maxCount = 1000, CancellationToken ct = default) {
            ct.ThrowIfCancellationRequested();

            try {
                var path = Path.Combine(Root, category);
                var items =
                    Directory.Exists(path)
                        ? Directory.GetDirectories(path)
                            .Select(x => x.Substring(path.Length + 1))
                            .Where(x => after == "" || string.CompareOrdinal(x, after) > 0)
                            .OrderBy(x => x)
                            .Take(maxCount)
                            .ToArray()
                        : Array.Empty<string>();
                
                return Task.FromResult(new Result<string[]>(items));
            }
            catch (Exception e) {
                return Task.FromResult(Result<string[]>.Exception(e));
            }
        }

        private string GetMemPath(EntityId id) => Path.Combine(Root, id.Id);

        private string GetMemDirectory(StampKey key) => Path.Combine(Root, key.EntId.Id);

        private string GetMemFileMask(StampKey key) =>
            key.IncId.Id + ".*";

        private string GetEventFileName(StampKey key) =>
            key.IncId.Id + ".evt";

        private (IncrementId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new IncrementId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(StampKey key, string encoding) => 
            Path.Combine(Root, key.EntId.Id, key.IncId.Id + "." + encoding);
    }
}