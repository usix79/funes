using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Funes.Fs {
    
    public class FileSystemRepository : IRepository {
        
        public string Root { get; }
        
        public FileSystemRepository(string root) => Root = root; 

        public async Task<ReflectionId> GetLatestRid(MemKey key) {
            var fileName = GetLatestFileName(key);
            return
                File.Exists(fileName)
                    ? new ReflectionId {Id = await File.ReadAllTextAsync(fileName)}
                    : ReflectionId.Null;
        }

        public async Task SetLatestRid(MemKey key, ReflectionId rid) {
            Directory.CreateDirectory(GetLatestPath(key));
            await File.WriteAllTextAsync(GetLatestFileName(key), rid.Id);
        }

        public async Task<Mem?> GetMem(MemKey key, ReflectionId reflectionId) {
            var fileName = GetMemFileName(key, reflectionId);
            return
                File.Exists(fileName)
                    ? new Mem(key, await ReadHeaders(fileName), File.OpenRead(fileName))
                    : null;
        }

        public async Task PutMem(Mem mem, ReflectionId reflectionId) {
            Directory.CreateDirectory(GetMemPath(mem.Key));
            var fileName = GetMemFileName(mem.Key, reflectionId);
            await using FileStream fs = File.OpenWrite(fileName);
            await mem.Content.CopyToAsync(fs);
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

        private string GetMemFileName(MemKey key, ReflectionId rid) 
            => Path.Combine(Root, key.Category, key.Id, rid.Id);

        private string GetLatestPath(MemKey key) => Path.Combine(Root, "_latest", key.Category);
        private string GetLatestFileName(MemKey key) => Path.Combine(Root, "_latest", key.Category, key.Id);

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