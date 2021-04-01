namespace Funes.Tests {
    public record Simple(int Id, string Name);

    public abstract record Msg() {
        public record Append(string Txt);

        public record Clear();
    }
}