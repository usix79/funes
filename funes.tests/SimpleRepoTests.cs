using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Funes.Impl;
using Xunit;

namespace Funes.Tests {
    
    public class SimpleRepoTests : AbstractRepoTests{
        protected override IRepository CreateRepo() {
            return new SimpleRepository();
        }
    }
}