using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Toolkit.HighPerformance;

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
        
        public async Task<Result<Void>> Save(BinaryStamp stamp, CancellationToken ct) {
            try {
                var encoding = stamp.Data.Encoding;
                var req = new PutObjectRequest {
                    BucketName = BucketName,
                    Key = CreateMemS3Key(stamp.Key),
                    ContentType = ResolveContentType(),
                    InputStream = stamp.Data.Memory.AsStream(),
                    Headers = {[EncodingKey] = encoding}
                };
                
                var _ = await _client.PutObjectAsync(req, ct);

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
        
        public async Task<Result<BinaryStamp>> Load(StampKey key, CancellationToken ct) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key), ct);
                
                var encoding = resp.Metadata[EncodingKey];

                await using MemoryStream stream = new();
                await resp.ResponseStream.CopyToAsync(stream, ct);
                if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                return new Result<BinaryStamp>(new BinaryStamp(key, new BinaryData(encoding, buffer)));
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) { return Result<BinaryStamp>.NotFound; }
            catch (AmazonS3Exception e) { return Result<BinaryStamp>.IoError(e.ToString()); }
            catch (Exception e) { return Result<BinaryStamp>.Exception(e); }
        }
        
        public async Task<Result<ReadOnlyMemory<byte>>> LoadBinary(StampKey key, CancellationToken ct) {
            try {
                var resp = await _client.GetObjectAsync(BucketName, CreateMemS3Key(key), ct);
                await using var stream = new MemoryStream();
                await resp.ResponseStream.CopyToAsync(stream, ct);
                if (!stream.TryGetBuffer(out var buffer)) buffer = stream.ToArray();
                return new Result<ReadOnlyMemory<byte>>(buffer);
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound) { return Result<ReadOnlyMemory<byte>>.NotFound; }
            catch (AmazonS3Exception e) { return Result<ReadOnlyMemory<byte>>.IoError(e.ToString()); }
            catch (Exception e) { return Result<ReadOnlyMemory<byte>>.Exception(e); }
        }

        public async Task<Result<IncrementId[]>> HistoryBefore(EntityId eid,
            IncrementId before, int maxCount = 1, CancellationToken ct = default) {
            try {
                var prefix = CreateMemS3Id(eid);
                var req = new ListObjectsV2Request {
                    BucketName = BucketName,
                    Prefix = prefix,
                    StartAfter = CreateMemS3Key(new StampKey(eid, before)),
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

        public async Task<Result<IncrementId[]>> HistoryAfter(EntityId eid,
            IncrementId after, CancellationToken ct = default) {
            
            try {
                List<IncrementId> ids = new();
                var prefix = CreateMemS3Id(eid);
                var startId = IncrementId.Singularity;
                while (true) {
                    var req = new ListObjectsV2Request {
                        BucketName = BucketName,
                        Prefix = prefix,
                        StartAfter = CreateMemS3Key(new StampKey(eid, startId)),
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

        public async Task<Result<string[]>> List(string category, string after = "", int maxCount = 1000, CancellationToken ct = default) {
            try {
                var prefix = category != "" ? $"{Prefix}/{category}/" : $"{Prefix}/"; 
                var req = new ListObjectsV2Request {
                    BucketName = BucketName,
                    Prefix = prefix,
                    Delimiter = "/",
                    StartAfter = after != "" ? $"{prefix}{after}/" : null,
                    MaxKeys = maxCount
                };

                var resp = await _client.ListObjectsV2Async(req, ct);
                
                var arr = new string[resp!.CommonPrefixes.Count];
                for (var i = 0; i < arr.Length; i++) {
                    var fullPrefix = resp!.CommonPrefixes[i];
                    arr[i] = fullPrefix.Substring(prefix.Length, fullPrefix.Length - prefix.Length - 1); 
                }

                return new Result<string[]>(arr);
            }
            catch (TaskCanceledException) { throw; }
            catch (AmazonS3Exception e) { return Result<string[]>.IoError(e.ToString()); }
            catch (Exception e) { return Result<string[]>.Exception(e); }
        }

        private string CreateMemS3Key(StampKey key) => $"{Prefix}/{key.EntId.Id}/{key.IncId.Id}";
        private string CreateMemS3Id(EntityId id) => $"{Prefix}/{id.Id}/";
    }
}