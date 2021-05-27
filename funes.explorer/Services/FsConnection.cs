using Funes.Fs;

namespace Funes.Explorer.Services {
    public class FsConnection : IFunesConnection {
        public string Description { get; }
        public IRepository Repo { get; }

        public FsConnection(string root) {
            Repo = new FileSystemRepository(root);
            Description = $"File System Connection: {root}";
        }
    }
}