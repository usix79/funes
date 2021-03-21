using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root;

        public async ValueTask<Result<bool>> Put<T>(Mem<T> mem, IRepository.Encoder<T> encoder) {
            try {
                Directory.CreateDirectory(GetMemPath(mem.Key.Id));
                var fileName = GetMemFileName(mem.Key);
                await using FileStream fs = File.OpenWrite(fileName);
                var encodeResult = await encoder(fs, mem.Content);
                if (encodeResult.IsError) return new Result<bool>(encodeResult.Error);

                await WriteHeaders(fileName, encodeResult.Value, mem.Headers);
                
                return new Result<bool>(true);
            }
            catch (Exception e) {
                return Result<bool>.Exception(e);
            }
        }

        public async ValueTask<Result<Mem<T>>> Get<T>(MemKey key, IRepository.Decoder<T> decoder) {
            try {
                var fileName = GetMemFileName(key);

                if (File.Exists(fileName)) {
                    var (encoding, headers) = await ReadHeaders(fileName);
                    var decodeResult = await decoder(File.OpenRead(fileName), encoding);
                    if (decodeResult.IsError) return new Result<Mem<T>>(decodeResult.Error);
                    return new Result<Mem<T>>(new Mem<T>(key, headers, decodeResult.Value));
                } 
            
                return Result<Mem<T>>.NotFound;
            }
            catch (Exception e) {
                return Result<Mem<T>>.Exception(e);
            }
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
            try {
                var path = GetMemPath(id);

                var rids =
                    Directory.GetFiles(path)
                        .Where(name => !name.EndsWith(HeadersExtension))
                        .Select(Path.GetFileName)
                        .OrderBy(x => x)
                        .SkipWhile(name => string.CompareOrdinal(before.Id, name) >= 0)
                        .Take(maxCount)
                        .Select(name => new ReflectionId(name!));

                return ValueTask.FromResult(new Result<IEnumerable<ReflectionId>>(rids));
            }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IEnumerable<ReflectionId>>.Exception(e));
            }
        }

        private string GetMemPath(MemId id) => Path.Combine(Root, id.Category, id.Name);

        private string GetMemFileName(MemKey key) => 
            Path.Combine(Root, key.Id.Category, key.Id.Name, key.Rid.Id);

        private const string KwSeparator = "__=__";
        private const string EncodingKey = "encoding";
        private const string HeadersExtension = ".__headers";

        private string HeadersFileName(string mainFileName) => mainFileName + HeadersExtension;

        private async ValueTask WriteHeaders(string mainFileName, string contentType, IReadOnlyDictionary<string, string>? headers) {
            var txt = new StringBuilder();
            txt.Append(EncodingKey).Append(KwSeparator).AppendLine(contentType);
            if (headers?.Count > 0)
                foreach (var key in headers.Keys)
                    txt.Append(key).Append(KwSeparator).AppendLine(headers[key]);

            await File.WriteAllTextAsync(HeadersFileName(mainFileName), txt.ToString());
        }

        private async ValueTask<(string, Dictionary<string,string>?)> ReadHeaders(string mainFileName) {
            var headersFileName = HeadersFileName(mainFileName);

            var encoding = "???";
            Dictionary<string,string>? coll = null;
            if (File.Exists(headersFileName)) {
                foreach (var line in await File.ReadAllLinesAsync(headersFileName)) {
                    var parts = line.Split(KwSeparator);
                    var key = parts[0];
                    var value = parts.Length > 1 ? parts[1] : "";
                    if (key == EncodingKey) {
                        encoding = value;
                    }
                    else {
                        if (coll == null) coll = new();
                        coll[key] = value;
                    }
                }
            }
            return (encoding,coll);
        }
    }
}