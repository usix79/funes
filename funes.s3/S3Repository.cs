﻿using System;
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

        private const string EncodingKey = "x-amz-meta-encoding";
        
        public async ValueTask<Result<bool>> Put(EntityStamp entityStamp, ISerializer ser) {
            try {
                await using var stream = new MemoryStream();
                var encodeResult = await ser.Encode(stream, entityStamp.Value);
                if (encodeResult.IsError) return new Result<bool>(encodeResult.Error);

                var encoding = encodeResult.Value;
                stream.Position = 0;

                var req = new PutObjectRequest {
                    BucketName = BucketName,
                    Key = CreateMemS3Key(entityStamp.Key),
                    ContentType = ResolveContentType(),
                    InputStream = stream,
                    Headers = {[EncodingKey] = encoding}
                };
                
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
        public async ValueTask<Result<EntityStamp>> Get(EntityStampKey key, ISerializer ser) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key));
                
                var encoding = resp.Metadata[EncodingKey];

                var decodeResult = await ser.Decode(resp.ResponseStream, key.Eid, encoding);
                if (decodeResult.IsError) return new Result<EntityStamp>(decodeResult.Error);

                return new Result<EntityStamp>(new EntityStamp(key, decodeResult.Value));
            }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) {
                return Result<EntityStamp>.NotFound;
            }
            catch (AmazonS3Exception e) {
                return Result<EntityStamp>.IoError(e.ToString());
            }
            catch (Exception e) {
                return Result<EntityStamp>.Exception(e);
            }
        }
        
        public async ValueTask<Result<IEnumerable<ReflectionId>>> GetHistory(EntityId id, ReflectionId before, int maxCount = 1) {
            try {
                var req = new ListObjectsV2Request {
                    BucketName = BucketName,
                    Prefix = CreateMemS3Id(id),
                    StartAfter = CreateMemS3Key(new EntityStampKey(id, before)),
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

        private string CreateMemS3Key(EntityStampKey key)
            => $"{Prefix}/{key.Eid.Category}/{key.Eid.Name}/{key.Rid.Id}";
        
        private string CreateMemS3Id(EntityId id)
            => $"{Prefix}/{id.Category}/{id.Name}/";
    }
}