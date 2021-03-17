using System;
using System.Collections.Generic;
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

        public async Task<ReflectionId> GetLatestRid(MemKey key) {
            var result = await _client.GetAsync(CreateKeyForLatest(key)+"AAA");
            if (result.Success) {
                return new ReflectionId {Id = (string) result.Value};
            }

            return ReflectionId.Null;
        }

        public async Task SetLatestRid(MemKey key, ReflectionId rid) {
            var result = await _client.SetAsync(CreateKeyForLatest(key), rid.Id, _expirationTime);
        }

        public Task<Mem?> GetMem(MemKey key, ReflectionId reflectionId) {
            throw new NotImplementedException();
        }

        public Task PutMem(Mem mem, ReflectionId reflectionId) {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1) {
            throw new NotImplementedException();
        }

        public string CreateKeyForLatest(MemKey key)
            => $"_latest/{key.Category}/{key.Id}";
    }
}