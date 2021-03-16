using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root; 

        public async Task<(Mem,ReflectionId)?> GetLatest(MemKey key) {
            var path = GetMemPath(key);
            var fileName =
                Directory.GetFiles(path)
                    .Where(name => !name.EndsWith(HeadersExtention))
                    .OrderBy(x => x)
                    .FirstOrDefault();
            
            if (fileName != null) {
                var rid = new ReflectionId {Id = Path.GetFileName(fileName)};
                var mem = await Get(key, rid);
                if (mem != null) {
                    return (mem, rid);
                }
            }

            return null;
        }

        public async Task<Mem?> Get(MemKey key, ReflectionId reflectionId) {
            var path = GetMemPath(key);
            var fileName = Path.Combine(path, reflectionId.Id);

            if (File.Exists(fileName)) {
                var data = await File.ReadAllBytesAsync(fileName);
                var headers = await ReadHeaders(fileName);
                return new Mem(key, headers, data);
            }
            else {
                return null;
            }
        }

        public async Task Put(Mem mem, ReflectionId reflectionId) {
            var path = GetMemPath(mem.Key);
            Directory.CreateDirectory(path);
            var fileName = Path.Combine(path, reflectionId.Id);
            await File.WriteAllBytesAsync(fileName, mem.Data);
            await WriteHeaders(fileName, mem.Headers);
        }

        public Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1) {
            var path = GetMemPath(key);

            var result =
                Directory.GetFiles(path)
                    .Where(name => !name.EndsWith(HeadersExtention))
                    .Select(Path.GetFileName)
                    .OrderBy(x => x)
                    .SkipWhile(name => string.CompareOrdinal(before.Id, name) >= 0)
                    .Take(maxCount)
                    .Select(name => new ReflectionId {Id = name!});

            return Task.FromResult(result);
        }

        private string GetMemPath(MemKey key) => Path.Combine(Root, key.Category, key.Id);

        private const string KwSeparator = "__=__";

        private const string HeadersExtention = ".__headers";

        private string HeadersFileName(string mainFileName)
            => mainFileName + HeadersExtention;

        private async Task WriteHeaders(string mainFileName, NameValueCollection headers) {
            if (headers.Count > 0) {
                var txt = new StringBuilder();
                foreach (var key in headers.AllKeys) {
                    txt.Append(key);
                    txt.Append(KwSeparator);
                    txt.AppendLine(headers[key]);
                }

                await File.WriteAllTextAsync(HeadersFileName(mainFileName), txt.ToString());
            }
        }

        private async Task<NameValueCollection> ReadHeaders(string mainFileName) {
            var headersFileName = HeadersFileName(mainFileName);

            var coll = new NameValueCollection();
            if (File.Exists(headersFileName)) {
                foreach (var line in await File.ReadAllLinesAsync(headersFileName)) {
                    var parts = line.Split(KwSeparator);
                    var key = parts[0];
                    var value = parts.Length > 1 ? parts[1] : "";
                    coll[key] = value;
                }
            }
            return coll;
        }
    }
}