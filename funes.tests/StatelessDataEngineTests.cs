using System;
using Funes.Impl;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace Funes.Tests {
    public class StatelessDataEngineTests : AbstractDataEngineTests {
        
        public StatelessDataEngineTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper) {
        }
        protected override IDataEngine CreateEngine(IRepository repo, ICache cache, ITransactionEngine tre, ILogger logger) {
            return new StatelessDataEngine(repo, cache, tre, logger);
        }

    }
}