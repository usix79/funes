using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using static Funes.Tests.TestHelpers;

namespace Funes.Tests {
    public abstract class AbstractTreTests {
        protected abstract ITransactionEngine CreateEngine();

        private async Task AssertCommit(ITransactionEngine tre, bool expectedSuccess, 
            StampKey[] inputs, EntityId[] outputs, string incId) {
            var commitResult = await tre.TryCommit(inputs, outputs, new IncrementId(incId), default);
            if (expectedSuccess) {
                Assert.True(commitResult.IsOk, commitResult.Error.ToString());
            }
            else {
                Assert.True(commitResult.Error is Error.CommitError);
            }
        }

        [Fact]
        public async void EmptyTest() {
            var tre = CreateEngine();

            await AssertCommit(tre, true, EmptyKeys, EntIds(), "100");

            var eid = CreateRandomEntId(); 
            // if a transaction engine doesn't know about premise, it should treat is as truth 
            await AssertCommit(tre, true, Keys((eid, "100500")), EntIds(eid), "100");
        }

        [Fact]
        public async void SingleEntityTest() {
            var tre = CreateEngine();
            var eid = CreateRandomEntId();
            await AssertCommit(tre, true, EmptyKeys, EntIds(eid), "100500");
            await AssertCommit(tre, false, Keys((eid, "100499")), EntIds(eid), "100499");
            await AssertCommit(tre, true, Keys((eid, "100500")), EntIds(eid), "100500");
            await AssertCommit(tre, true, Keys((eid, "100500")), EntIds(eid), "100499");
            await AssertCommit(tre, true, Keys((eid, "100499")), EntIds(), "100");
            await AssertCommit(tre, true, Keys((eid, "100499")), EntIds(eid), "100501");
            await AssertCommit(tre, false, Keys((eid, "100500")), EntIds(eid), "100499");
        }

        [Fact]
        public async void MultipleEntityTest() {
            var tre = CreateEngine();
            var eid1 = CreateRandomEntId();
            var eid2 = CreateRandomEntId();
            await AssertCommit(tre, true, EmptyKeys, EntIds(eid1, eid2), "100500");
            await AssertCommit(tre, true, Keys((eid1, "100500"), (eid2, "100500")), EntIds(eid1), "100499");
            await AssertCommit(tre, true, Keys((eid1, "100499"), (eid2, "100500")), EntIds(), "100");
            await AssertCommit(tre, false, Keys((eid1, "100500"), (eid2, "100500")), EntIds(eid1, eid2), "100501");
            await AssertCommit(tre, false, Keys((eid1, "100499"), (eid2, "100499")), EntIds(eid1, eid2), "100501");
            await AssertCommit(tre, true, Keys((eid1, "100499"), (eid2, "100500")), EntIds(eid1, eid2),"100501");
            await AssertCommit(tre, true, Keys((eid1, "100501"), (eid2, "100501")), EntIds(), "100");
        }
    }
}