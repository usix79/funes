using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root;

        public async ValueTask<Result<bool>> Put(EntityStamp entityStamp, ISerializer ser) {
            await using MemoryStream stream = new();
            var encoderResult = await ser.Encode(stream, entityStamp.Entity.Id, entityStamp.Value);
            if (encoderResult.IsError) return new Result<bool>(encoderResult.Error);
            return await Put(entityStamp.Key, stream.GetBuffer(), encoderResult.Value);
        }

        public async ValueTask<Result<bool>> Put(EntityStampKey key, ReadOnlyMemory<byte> value, string encoding) {
            try {
                Directory.CreateDirectory(GetMemPath(key.Eid));
                var fileName = GetMemFileName(key, encoding);
                await using FileStream fs = File.OpenWrite(fileName);
                await fs.WriteAsync(value);
                return new Result<bool>(true);
            }
            catch (Exception e) {
                return Result<bool>.Exception(e);
            }
        }

        public async ValueTask<Result<EntityStamp>> Get(EntityStampKey key, ISerializer ser) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    foreach (var fileName in Directory.GetFiles(memDirectory, GetMemFileMask(key))) {
                        var (rid, encoding) = ParseFileName(fileName);
                        var decodeResult = await ser.Decode(File.OpenRead(fileName), key.Eid, encoding);

                        if (decodeResult.IsOk) 
                            return new Result<EntityStamp>(new EntityStamp(key, decodeResult.Value));
                    
                        if (decodeResult.Error is not Error.NotSupportedEncodingError)
                            return new Result<EntityStamp>(decodeResult.Error);
                    }
                }
                return Result<EntityStamp>.NotFound;
            }
            catch (Exception e) {
                return Result<EntityStamp>.Exception(e);
            }
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(EntityId id, ReflectionId before, int maxCount = 1) {
            try {
                var path = GetMemPath(id);

                var rids =
                    Directory.GetFiles(path)
                        .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                        .OrderBy(x => x)
                        .SkipWhile(rid => before.CompareTo(rid) >= 0)
                        .Take(maxCount);

                return ValueTask.FromResult(new Result<IEnumerable<ReflectionId>>(rids));
            }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IEnumerable<ReflectionId>>.Exception(e));
            }
        }

        private string GetMemPath(EntityId id) => Path.Combine(Root, id.Category, id.Name);

        private string GetMemDirectory(EntityStampKey key) => 
            Path.Combine(Root, key.Eid.Category, key.Eid.Name);

        private string GetMemFileMask(EntityStampKey key) =>
            key.Rid.Id + ".*";

        private (ReflectionId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new ReflectionId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(EntityStampKey key, string encoding) => 
            Path.Combine(Root, key.Eid.Category, key.Eid.Name, key.Rid.Id + "." + encoding);
    }
}