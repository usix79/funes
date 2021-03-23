using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : Mem.IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root;

        public async ValueTask<Result<bool>> Put(MemStamp memStamp, Mem.IRepository.Encoder encoder) {
            await using MemoryStream stream = new();
            var encoderResult = await encoder(stream, memStamp.Value);
            if (encoderResult.IsError) return new Result<bool>(encoderResult.Error);
            return await Put(memStamp.Key, stream.GetBuffer(), encoderResult.Value);
        }

        public async ValueTask<Result<bool>> Put(MemKey key, ReadOnlyMemory<byte> value, string encoding) {
            try {
                Directory.CreateDirectory(GetMemPath(key.Id));
                var fileName = GetMemFileName(key, encoding);
                await using FileStream fs = File.OpenWrite(fileName);
                await fs.WriteAsync(value);
                return new Result<bool>(true);
            }
            catch (Exception e) {
                return Result<bool>.Exception(e);
            }
        }

        public async ValueTask<Result<MemStamp>> Get(MemKey key, Mem.IRepository.Decoder decoder) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    foreach (var fileName in Directory.GetFiles(memDirectory, GetMemFileMask(key))) {
                        var (rid, encoding) = ParseFileName(fileName);
                        var decodeResult = await decoder(File.OpenRead(fileName), encoding);

                        if (decodeResult.IsOk) 
                            return new Result<MemStamp>(new MemStamp(key, decodeResult.Value));
                    
                        if (decodeResult.Error != Error.NotSupportedEncoding)
                            return new Result<MemStamp>(decodeResult.Error);
                    }
                }
                return Result<MemStamp>.NotFound;
            }
            catch (Exception e) {
                return Result<MemStamp>.Exception(e);
            }
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
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

        private string GetMemPath(MemId id) => Path.Combine(Root, id.Category, id.Name);

        private string GetMemDirectory(MemKey key) => 
            Path.Combine(Root, key.Id.Category, key.Id.Name);

        private string GetMemFileMask(MemKey key) =>
            key.Rid.Id + ".*";

        private (ReflectionId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new ReflectionId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(MemKey key, string encoding) => 
            Path.Combine(Root, key.Id.Category, key.Id.Name, key.Rid.Id + "." + encoding);
    }
}