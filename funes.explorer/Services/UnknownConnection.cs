using Funes.Impl;

namespace Funes.Explorer.Services {
    public class UnknownConnection : IFunesConnection {
        public string Description => "Unknown Connection";
        public IRepository Repo { get; } = new SimpleRepository();
    }
}