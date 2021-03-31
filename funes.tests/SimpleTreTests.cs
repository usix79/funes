using Funes.Impl;

namespace Funes.Tests {
    public class SimpleTreTests : AbstractTreTests {
        protected override ITransactionEngine CreateEngine() => new SimpleTransactionEngine();
    }
}