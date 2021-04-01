using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public abstract class AbstractTreTests {
        protected abstract ITransactionEngine CreateEngine();

        private async Task AssertCommit(ITransactionEngine tre, bool expectedSuccess, 
            IEnumerable<EntityStampKey> premises, IEnumerable<EntityStampKey> conclusions) {
            var commitResult = await tre.Commit(premises, conclusions, default);
            if (expectedSuccess) {
                Assert.True(commitResult.IsOk, commitResult.Error.ToString());
                Assert.True(commitResult.Value);
            }
            else {
                Assert.True(commitResult.Error is Error.TransactionError);
            }
        }
        
        private IEnumerable<EntityStampKey> Keys(params (EntityId,string)[] keys) => 
            keys.Select(x => new EntityStampKey(x.Item1, new CognitionId(x.Item2)));

        [Fact]
        public async void EmptyTest() {
            var tre = CreateEngine();

            await AssertCommit(tre, true, Keys(), Keys());

            var eid = CreateRandomEid(); 
            // if a transaction engine doesn't know about premise, it should treat is as truth 
            await AssertCommit(tre, true, Keys((eid, "100500")), Keys((eid, "100501")));
        }

        [Fact]
        public async void SingleEntityTest() {
            var tre = CreateEngine();
            var eid = CreateRandomEid();
            await AssertCommit(tre, true, Keys(), Keys((eid, "100500")));
            await AssertCommit(tre, false, Keys((eid, "100499")), Keys((eid, "100499")));
            await AssertCommit(tre, true, Keys((eid, "100500")), Keys((eid, "100500")));
            await AssertCommit(tre, true, Keys((eid, "100500")), Keys((eid, "100499")));
            await AssertCommit(tre, true, Keys((eid, "100499")), Keys());
            await AssertCommit(tre, true, Keys((eid, "100499")), Keys((eid, "100501")));
            await AssertCommit(tre, false, Keys((eid, "100500")), Keys((eid, "100499")));
        }

        [Fact]
        public async void MultipleEntityTest() {
            var tre = CreateEngine();
            var eid1 = CreateRandomEid();
            var eid2 = CreateRandomEid();
            await AssertCommit(tre, true, Keys(), Keys((eid1, "100500"), (eid2, "100500")));
            await AssertCommit(tre, true, Keys((eid1, "100500"), (eid2, "100500")), Keys((eid1, "100499")));
            await AssertCommit(tre, true, Keys((eid1, "100499"), (eid2, "100500")), Keys());
            await AssertCommit(tre, false, Keys((eid1, "100500"), (eid2, "100500")), Keys((eid1, "100501"), (eid2, "100501")));
            await AssertCommit(tre, false, Keys((eid1, "100499"), (eid2, "100499")), Keys((eid1, "100501"), (eid2, "100501")));
            await AssertCommit(tre, true, Keys((eid1, "100499"), (eid2, "100500")), Keys((eid1, "100501"), (eid2, "100501")));
            await AssertCommit(tre, true, Keys((eid1, "100501"), (eid2, "100501")), Keys());
        }
    }
}