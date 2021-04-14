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

        public async ValueTask<Result<Void>> Save(EntityStamp entityStamp, ISerializer ser, CancellationToken ct) {
            await using MemoryStream stream = new();
            var encoderResult = await ser.Encode(stream, entityStamp.Entity.Id, entityStamp.Value);
            if (encoderResult.IsError) return new Result<Void>(encoderResult.Error);
            return await Write(entityStamp.Key, stream.GetBuffer(), encoderResult.Value, ct);
        }
        
        public async ValueTask<Result<Void>> SaveEvent(EntityId eid, Event evt, CancellationToken ct) {
            return await Write(eid.CreateStampKey(evt.IncId), evt.Data, "evt", ct);
        }
        
        private async ValueTask<Result<Void>> Write(EntityStampKey key, 
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

        public async ValueTask<Result<Event>> LoadEvent(EntityStampKey key, CancellationToken ct) {
            try {
                var memDirectory = GetMemDirectory(key);
                if (Directory.Exists(memDirectory)) {
                    var fileName = GetEventFileName(key);
                    var fullFileName = Path.Combine(memDirectory, fileName);
                    if (File.Exists(fullFileName)) {
                        var data = await File.ReadAllBytesAsync(fullFileName, ct);
                        return new Result<Event>(new Event(key.IncId, data));
                    }
                }
                return Result<Event>.NotFound;
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception x) { return Result<Event>.Exception(x); }
        }
        
        public ValueTask<Result<IncrementId[]>> HistoryBefore(EntityId eid, 
                    IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            try {
                ct.ThrowIfCancellationRequested();
                
                var path = GetMemPath(eid);

                var incIds =
                    Directory.GetFiles(path)
                        .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                        .Where(before.IsNewerThan)
                        .OrderBy(x => x)
                        .Take(maxCount)
                        .ToArray();

                return ValueTask.FromResult(new Result<IncrementId[]>(incIds));
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IncrementId[]>.Exception(e));
            }
        }

        public ValueTask<Result<IncrementId[]>> HistoryAfter(EntityId eid, 
            IncrementId after, CancellationToken ct = default) {

            try {
                ct.ThrowIfCancellationRequested();
                
                var path = GetMemPath(eid);

                var incIds =
                    Directory.GetFiles(path)
                        .Select(name => ParseFileName(Path.GetFileName(name)).Item1)
                        .Where(after.IsOlderThan)
                        .OrderByDescending(x => x)
                        .ToArray();

                return ValueTask.FromResult(new Result<IncrementId[]>(incIds));
            }
            catch (TaskCanceledException) { throw; }
            catch (Exception e) {
                return ValueTask.FromResult(Result<IncrementId[]>.Exception(e));
            }
        }

        private string GetMemPath(EntityId id) => Path.Combine(Root, id.Id);

        private string GetMemDirectory(EntityStampKey key) => Path.Combine(Root, key.EntId.Id);

        private string GetMemFileMask(EntityStampKey key) =>
            key.IncId.Id + ".*";

        private string GetEventFileName(EntityStampKey key) =>
            key.IncId.Id + ".evt";

        private (IncrementId, string) ParseFileName(string fullFileName) {
            var fileName = Path.GetFileName(fullFileName);
            var parts = fileName.Split('.');
            return (new IncrementId(parts[0]), parts.Length > 1 ? parts[1] : "");
        }
        
        private string GetMemFileName(EntityStampKey key, string encoding) => 
            Path.Combine(Root, key.EntId.Id, key.IncId.Id + "." + encoding);
    }
}