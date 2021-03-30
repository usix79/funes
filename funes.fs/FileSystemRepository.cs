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
                Directory.CreateDirectory(GetMemPath(key.Eid));
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
                        var (cid, encoding) = ParseFileName(fileName);
                        var decodeResult = await ser.Decode(File.OpenRead(fileName), key.Eid, encoding);

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
        
        public ValueTask<Result<IEnumerable<CognitionId>>> History(EntityId id, 
                    CognitionId before, int maxCount = 1, CancellationToken ct = default) {
            try {
                var path = GetMemPath(id);

                var cids =
                    Directory.GetFiles(path)
                        .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                        .OrderBy(x => x)
                        .SkipWhile(cid => before.CompareTo(cid) >= 0)
                        .Take(maxCount);

                return ValueTask.FromResult(new Result<IEnumerable<CognitionId>>(cids));
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IEnumerable<CognitionId>>.Exception(e));
            }
        }

        private string GetMemPath(EntityId id) => Path.Combine(Root, id.Id);

        private string GetMemDirectory(EntityStampKey key) => Path.Combine(Root, key.Eid.Id);

        private string GetMemFileMask(EntityStampKey key) =>
            key.Cid.Id + ".*";

        private (CognitionId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new CognitionId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(EntityStampKey key, string encoding) => 
            Path.Combine(Root, key.Eid.Id, key.Cid.Id + "." + encoding);
    }
}