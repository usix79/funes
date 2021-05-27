using Funes.S3;

namespace Funes.Explorer.Services {
    public class S3Connection : IFunesConnection {
        public string Description { get; }
        public IRepository Repo { get; }

        public S3Connection(string path) {
            var bucketName = path;
            var prefix = "";
            var prefixIdx = path.IndexOf('/');
            if (prefixIdx != -1) {
                bucketName = path.Substring(0, prefixIdx);
                prefix = path.Substring(prefixIdx + 1);
            }

            Repo = new S3Repository(bucketName, prefix);
            Description = $"S3 Connection: {path}";
        }
    }
}