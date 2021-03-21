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

        private const string EncodingKey = "encoding";
        
        public async ValueTask<Result<bool>> Put<T>(Mem<T> mem, IRepository.Encoder<T> encoder) {
            try {
                await using var stream = new MemoryStream();
                var encodeResult = await encoder(stream, mem.Content);
                if (encodeResult.IsError) return new Result<bool>(encodeResult.Error);

                var encoding = encodeResult.Value;
                stream.Position = 0;

                var req = new PutObjectRequest {
                    BucketName = BucketName,
                    Key = CreateMemS3Key(mem.Key),
                    ContentType = ResolveContentType(),
                    InputStream = stream,
                    Metadata = {[EncodingKey] = encoding}
                };

                if (mem.Headers?.Count > 0) {
                    foreach (var key in mem.Headers.Keys) {
                        req.Metadata[key] = mem.Headers[key];
                    }
                }

                var resp = await _client.PutObjectAsync(req);

                return new Result<bool>(true);

                string ResolveContentType() =>
                    encoding switch {
                        var str when str.StartsWith("json") => "application/json",
                        _ => "application/octet-stream"
                    };
            }
            catch (AmazonS3Exception e) {
                return Result<bool>.IoError(e.ToString());
            }
            catch (Exception e) {
                return Result<bool>.Exception(e);
            }
        }
        
        public async ValueTask<Result<Mem<T>>> Get<T>(MemKey key, IRepository.Decoder<T> decoder) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key));
                
                
                var encoding = resp.Headers.ContentType;
                Dictionary<string, string>? headers = null;
                if (resp.Metadata.Keys.Count > 0) {
                    headers = new();

                    foreach (var headerKey in resp.Metadata.Keys) {
                        var actualKey =
                            headerKey.StartsWith("x-amz-meta-")
                                ? headerKey.Substring("x-amz-meta-".Length)
                                : headerKey;

                        var value = resp.Metadata[headerKey];
                        if (actualKey == EncodingKey) {
                            encoding = value;
                        }
                        else {
                            headers[actualKey] = resp.Metadata[headerKey];
                        }
                    }
                }

                var decodeResult = await decoder(resp.ResponseStream, encoding);
                if (decodeResult.IsError) return new Result<Mem<T>>(decodeResult.Error);

                return new Result<Mem<T>>(new Mem<T>(key, headers, decodeResult.Value));
            }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) {
                return Result<Mem<T>>.NotFound;
            }
            catch (AmazonS3Exception e) {
                return Result<Mem<T>>.IoError(e.ToString());
            }
            catch (Exception e) {
                return Result<Mem<T>>.Exception(e);
            }
        }
        
        public async ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(MemId id, ReflectionId before, int maxCount = 1) {
            try {
                var req = new ListObjectsV2Request {
                    BucketName = BucketName,
                    Prefix = CreateMemS3Id(id),
                    StartAfter = CreateMemS3Key(new MemKey(id, before)),
                    MaxKeys = maxCount
                };

                var resp = await _client.ListObjectsV2Async(req);

                return
                    new Result<IEnumerable<ReflectionId>>(
                        resp!.S3Objects
                            .Select(s3Obj => new ReflectionId (s3Obj.Key.Substring(req.Prefix.Length))));
            }
            catch (AmazonS3Exception e) {
                return Result<IEnumerable<ReflectionId>>.IoError(e.ToString());
            }
            catch (Exception e) {
                return Result<IEnumerable<ReflectionId>>.Exception(e);
            }
        }

        private string CreateMemS3Key(MemKey key)
            => $"{Prefix}/{key.Id.Category}/{key.Id.Name}/{key.Rid.Id}";
        
        private string CreateMemS3Id(MemId id)
            => $"{Prefix}/{id.Category}/{id.Name}/";
    }
}