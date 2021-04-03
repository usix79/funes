using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root;

        public async ValueTask<Result<bool>> Save(EntityStamp entityStamp, ISerializer ser, CancellationToken ct) {
            await using MemoryStream stream = new();
            var encoderResult = await ser.Encode(stream, entityStamp.Entity.Id, entityStamp.Value);
            if (encoderResult.IsError) return new Result<bool>(encoderResult.Error);
            return await Write(entityStamp.Key, stream.GetBuffer(), encoderResult.Value, ct);
        }

        private async ValueTask<Result<bool>> Write(EntityStampKey key, 
                        ReadOnlyMemory<byte> value, string encoding, CancellationToken ct) {
            try {
                Directory.CreateDirectory(GetMemPath(key.EntId));
                var fileName = GetMemFileName(key, encoding);
                await using FileStream fs = File.OpenWrite(fileName);
                await fs.WriteAsync(value, ct);
                return new Result<bool>(true);
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) { return Result<bool>.Exception(e); }
        }

        public async ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer ser, CancellationToken ct) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    foreach (var fileName in Directory.GetFiles(memDirectory, GetMemFileMask(key))) {
                        ct.ThrowIfCancellationRequested();
                        var (incId, encoding) = ParseFileName(fileName);
                        var decodeResult = await ser.Decode(File.OpenRead(fileName), key.EntId, encoding);

                        if (decodeResult.IsOk) 
                            return new Result<EntityStamp>(new EntityStamp(key, decodeResult.Value));
                    
                        if (decodeResult.Error is not Error.NotSupportedEncodingError)
                            return new Result<EntityStamp>(decodeResult.Error);
                    }
                }
                return Result<EntityStamp>.NotFound;
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception x) { return Result<EntityStamp>.Exception(x); }
        }
        
        public ValueTask<Result<IEnumerable<IncrementId>>> History(EntityId id, 
                    IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            try {
                var path = GetMemPath(id);

                var incIds =
                    Directory.GetFiles(path)
                        .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                        .OrderBy(x => x)
                        .SkipWhile(incId => before.CompareTo(incId) >= 0)
                        .Take(maxCount);

                return ValueTask.FromResult(new Result<IEnumerable<IncrementId>>(incIds));
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IEnumerable<IncrementId>>.Exception(e));
            }
        }

        private string GetMemPath(EntityId id) => Path.Combine(Root, id.Id);

        private string GetMemDirectory(EntityStampKey key) => Path.Combine(Root, key.EntId.Id);

        private string GetMemFileMask(EntityStampKey key) =>
            key.IncId.Id + ".*";

        private (IncrementId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new IncrementId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(EntityStampKey key, string encoding) => 
            Path.Combine(Root, key.EntId.Id, key.IncId.Id + "." + encoding);
    }
}