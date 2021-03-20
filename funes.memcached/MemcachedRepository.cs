using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;

namespace Funes.Memcached {
    
    public class MemcachedRepository : IRepository {

        private readonly IRepository _realRepo;
        private readonly IMemcachedClient _client;
        private readonly int _expirationTime = 60 * 60; // 1 hour;

        public MemcachedRepository(IRepository realRepo, string host = "localhost", int port = 11211) {
            _realRepo = realRepo;

            var loggerFactory = new Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory();
            var opt = new MemcachedClientOptions();
            var cfg = new MemcachedClientConfiguration(loggerFactory, opt);
            cfg.AddServer(host, port);
            cfg.Protocol = MemcachedProtocol.Binary;
            _client = new MemcachedClient(loggerFactory, cfg);
        }

        private const int EncodingSize = 16;

        public ValueTask<Result<bool>> Put<T>(Mem<T> mem, ReflectionId rid, IRepository.Encoder<T> encoder) {
            throw new NotImplementedException();
            
            // await using var stream = new MemoryStream();
            // await using var writer = new BinaryWriter(stream);
            // writer.Seek(EncodingSize, SeekOrigin.Begin); // reserve space for encoding
            // var encoding = await encoder(stream, mem.Content);
            // if (encoding.Length > 15)
            //     throw new FormatException($"Expected encoding name less than 15 characters, but got {encoding}");
            //
            // // encoding
            // writer.Seek(0, SeekOrigin.Begin);
            // writer.Write((byte)encoding.Length);
            // foreach (var ch in encoding)
            //     writer.Write((byte)ch);
            //
            // if (mem.Headers is not null) {
            //     writer.Seek(0, SeekOrigin.End);
            //     
            //     writer.Write(mem.Headers.Count);
            //     foreach (var pair in mem.Headers!) {
            //         writer.Write(pair.Key);
            //         writer.Write(pair.Value);
            //     }
            // }
            //
            // var key = CreateKey(mem.Id, rid);
            // if (!stream.TryGetBuffer(out var buffer))
            //     throw new FormatException("Cann''t extract buffer from stream");
            //
            // var success = await _client.SetAsync(key, buffer, _expirationTime);


            // Span<byte> xxx = stackalloc byte[21];
            // var successHistory = _client.Prepend(historyKey, xxx.ToArray());
            //stream.GetBuffer()

            // _realRepo.PutMem(mem, rid, (output, _) => {
            //     output.WriteAsync(buffer.Array!, EncodingSize, (int) stream.Position);
            //     return default;
            // });
        }

        
        public ValueTask<Result<Mem<T>>> Get<T>(MemId id, ReflectionId reflectionId, IRepository.Decoder<T> _) {
            throw new NotImplementedException();
        }
        
        public ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
            return _realRepo.GetHistory(id, before, maxCount);
        }

        public string CreateKey(MemId id, ReflectionId rid)
            => $"{id.Category}/{id.Name}/{rid}";

    }
}