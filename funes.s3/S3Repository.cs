using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Toolkit.HighPerformance;
using Microsoft.Toolkit.HighPerformance.Streams;

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
        
        public async ValueTask<Result<Void>> Save(EntityStamp entityStamp, ISerializer ser, CancellationToken ct) {
            try {
                await using var stream = new MemoryStream();
                var encodeResult = await ser.Encode(stream, entityStamp.Entity.Id,  entityStamp.Value);
                if (encodeResult.IsError) return new Result<Void>(encodeResult.Error);

                var encoding = encodeResult.Value;
                stream.Position = 0;

                var req = new PutObjectRequest {
                    BucketName = BucketName,
                    Key = CreateMemS3Key(entityStamp.Key),
                    ContentType = ResolveContentType(),
                    InputStream = stream,
                    Headers = {[EncodingKey] = encoding}
                };
                
                var resp = await _client.PutObjectAsync(req, ct);

                return new Result<Void>(Void.Value);

                string ResolveContentType() =>
                    encoding switch {
                        var str when str.StartsWith("json") => "application/json",
                        _ => "application/octet-stream"
                    };
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) { return Result<Void>.IoError(e.ToString()); }
            catch (Exception e) { return Result<Void>.Exception(e); }
        }
        
        public async ValueTask<Result<Void>> SaveEvent(EntityId eid, Event evt, CancellationToken ct) {
            try {
                var req = new PutObjectRequest {
                    BucketName = BucketName,
                    Key = CreateMemS3Key(eid.CreateStampKey(evt.IncId)),
                    ContentType = "application/octet-stream",
                    InputStream = evt.Data.AsStream(),
                    Headers = {[EncodingKey] = "evt"}
                };
                
                var resp = await _client.PutObjectAsync(req, ct);

                return new Result<Void>(Void.Value);
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) { return Result<Void>.IoError(e.ToString()); }
            catch (Exception e) { return Result<Void>.Exception(e); }
        }

        public async ValueTask<Result<EntityStamp>> Load(EntityStampKey key, ISerializer ser, CancellationToken ct) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key), ct);
                
                var encoding = resp.Metadata[EncodingKey];

                var decodeResult = await ser.Decode(resp.ResponseStream, key.EntId, encoding);
                return decodeResult.IsOk
                    ? new Result<EntityStamp>(new EntityStamp(key, decodeResult.Value))
                    : new Result<EntityStamp>(decodeResult.Error);
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) { return Result<EntityStamp>.NotFound; }
            catch (AmazonS3Exception e) { return Result<EntityStamp>.IoError(e.ToString()); }
            catch (Exception e) { return Result<EntityStamp>.Exception(e); }
        }
        
        public async ValueTask<Result<Event>> LoadEvent(EntityStampKey key, CancellationToken ct) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key), ct);
                await using var stream = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(stream, ct);
                if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                return new Result<Event>(new Event(key.IncId, buffer));
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) { return Result<Event>.NotFound; }
            catch (AmazonS3Exception e) { return Result<Event>.IoError(e.ToString()); }
            catch (Exception e) { return Result<Event>.Exception(e); }
        }

        public async ValueTask<Result<IncrementId[]>> HistoryBefore(EntityId eid, 
                        IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            try {
                var prefix = CreateMemS3Id(eid);
                var req = new ListObjectsV2Request {
                    BucketName = BucketName,
                    Prefix = prefix,
                    StartAfter = CreateMemS3Key(new EntityStampKey(eid, before)),
                    MaxKeys = maxCount
                };

                var resp = await _client.ListObjectsV2Async(req, ct);

                var arr = new IncrementId[resp!.S3Objects.Count];
                for (var i = 0; i < arr.Length; i++) {
                    arr[i] = new IncrementId(resp!.S3Objects[i].Key.Substring(prefix.Length));
                }

                return new Result<IncrementId[]>(arr);
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) { return Result<IncrementId[]>.IoError(e.ToString()); }
            catch (Exception e) { return Result<IncrementId[]>.Exception(e); }
        }

        public async ValueTask<Result<IncrementId[]>> HistoryAfter(EntityId eid, 
            IncrementId after, CancellationToken ct = default) {
            
            try {
                List<IncrementId> ids = new();
                var prefix = CreateMemS3Id(eid);
                var startId = IncrementId.Singularity;
                while (true) {
                    var req = new ListObjectsV2Request {
                        BucketName = BucketName,
                        Prefix = prefix,
                        StartAfter = CreateMemS3Key(new EntityStampKey(eid, startId)),
                        MaxKeys = 42
                    };

                    var resp = await _client.ListObjectsV2Async(req, ct);

                    if (resp!.S3Objects.Count == 0) break;

                    bool finish = false;
                    foreach (var s3Obj in resp!.S3Objects) {
                        var incId = new IncrementId(s3Obj.Key.Substring(prefix.Length));
                        if (after.CompareTo(incId) <= 0) {
                            finish = true;
                            break;
                        }
                        ids.Add(incId);
                    }

                    if (finish) break;
                    
                    startId = ids[^1];
                }

                ids.Reverse();
                return new Result<IncrementId[]>(ids.ToArray());
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) { return Result<IncrementId[]>.IoError(e.ToString()); }
            catch (Exception e) { return Result<IncrementId[]>.Exception(e); }
        }

        private string CreateMemS3Key(EntityStampKey key) => $"{Prefix}/{key.EntId.Id}/{key.IncId.Id}";
        private string CreateMemS3Id(EntityId id) => $"{Prefix}/{id.Id}/";
    }
}