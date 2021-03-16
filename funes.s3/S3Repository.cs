using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
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
        
        public async Task<(Mem, ReflectionId)?> GetLatest(MemKey key) {
            var req = new ListObjectsV2Request {
                BucketName = BucketName,
                Prefix = CombineS3Prefix(key),
                MaxKeys = 1
            };

            var resp = await _client.ListObjectsV2Async(req);

            var s3Obj = resp?.S3Objects.FirstOrDefault();
            
            if (s3Obj != null) {
                var rid = new ReflectionId {Id = s3Obj.Key.Substring(req.Prefix.Length)};
                var mem = await Get(key, rid);
                if (mem != null) {
                    return (mem, rid);
                }
            }
            
            return null;
        }

        public async Task<Mem?> Get(MemKey key, ReflectionId reflectionId) {

            try {

                var resp = await _client.GetObjectAsync(BucketName, CombineS3Key(key, reflectionId));

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

        public async Task Put(Mem mem, ReflectionId reflectionId) {

            var req = new PutObjectRequest {
                BucketName = BucketName,
                Key = CombineS3Key(mem.Key, reflectionId),
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
                Prefix = CombineS3Prefix(key),
                StartAfter = CombineS3Key(key, before),
                MaxKeys = maxCount
            };

            var resp = await _client.ListObjectsV2Async(req);

            return
                resp!.S3Objects
                    .Select(s3Obj => new ReflectionId {Id = s3Obj.Key.Substring(req.Prefix.Length)});
        }

        private string CombineS3Key(MemKey key, ReflectionId rid)
            => $"{Prefix}/{key.Category}/{key.Id}/{rid.Id}";

        private string CombineS3Prefix(MemKey key)
            => $"{Prefix}/{key.Category}/{key.Id}/";
    }
}