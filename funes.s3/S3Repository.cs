using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;

namespace Funes.S3 {
    
    public class S3Repository : IRepository {
        
        public string BucketName { get; }
        
        public string Prefix { get; }

        private readonly AmazonS3Client _client;

        public S3Repository(string bucketName, string prefix) {
            BucketName = bucketName;
            Prefix = prefix;
            _client = new AmazonS3Client();
        }
        
        public async Task<ReflectionId> GetLatestRid(MemKey key) {
            try {

                var resp = await _client.GetObjectAsync(BucketName, CreateLatestS3Key(key));

                if (resp != null) {
                    using StreamReader reader = new (resp.ResponseStream);
                    return new ReflectionId {Id = await reader.ReadToEndAsync()};
                }

                return ReflectionId.Null;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound) {
                return ReflectionId.Null;
            }
        }

        public async Task SetLatestRid(MemKey key, ReflectionId rid) {
            var req = new PutObjectRequest {
                BucketName = BucketName,
                Key = CreateLatestS3Key(key),
                InputStream = new MemoryStream(Encoding.UTF8.GetBytes(rid.Id))
            };

            var resp = await _client.PutObjectAsync(req);
        }

        public async Task<Mem?> GetMem(MemKey key, ReflectionId reflectionId) {

            try {

                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key, reflectionId));

                if (resp != null) {
                    var headers = new NameValueCollection();
                    foreach (var headerKey in resp.Metadata.Keys) {
                        var actualKey =
                            headerKey.StartsWith("x-amz-meta-") 
                            ? headerKey.Substring("x-amz-meta-".Length)
                            : headerKey;
                        
                        headers[actualKey] = resp.Metadata[headerKey];
                    }

                    return new Mem(key, headers, resp.ResponseStream);
                }

                return null;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound) {
                return null;
            }
        }

        public async Task PutMem(Mem mem, ReflectionId reflectionId) {

            var req = new PutObjectRequest {
                BucketName = BucketName,
                Key = CreateMemS3Key(mem.Key, reflectionId),
                InputStream = mem.Content
            };

            if (mem.Headers?.Count > 0) {
                foreach (var key in mem.Headers.AllKeys) {
                    req.Metadata[key] = mem.Headers[key];
                }
            }

            var resp = await _client.PutObjectAsync(req);
        }

        public async Task<IEnumerable<ReflectionId>> GetHistory(MemKey key, ReflectionId before, int maxCount = 1) {
            
            var req = new ListObjectsV2Request {
                BucketName = BucketName,
                Prefix = CreateMemS3Prefix(key),
                StartAfter = CreateMemS3Key(key, before),
                MaxKeys = maxCount
            };

            var resp = await _client.ListObjectsV2Async(req);

            return
                resp!.S3Objects
                    .Select(s3Obj => new ReflectionId {Id = s3Obj.Key.Substring(req.Prefix.Length)});
        }

        private string CreateMemS3Key(MemKey key, ReflectionId rid)
            => $"{Prefix}/{key.Category}/{key.Id}/{rid.Id}";
        
        private string CreateMemS3Prefix(MemKey key)
            => $"{Prefix}/{key.Category}/{key.Id}/";

        private string CreateLatestS3Key(MemKey key)
            => $"{Prefix}/_latest/{key.Category}/{key.Id}";
    }
}