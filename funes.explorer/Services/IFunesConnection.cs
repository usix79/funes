namespace Funes.Explorer.Services {
    public interface IFunesConnection {
        
        public string Description { get; }
        public IRepository Repo { get; }
    }
}