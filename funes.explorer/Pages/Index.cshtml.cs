using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

namespace Funes.Explorer.Pages {
    public class IndexModel : PageModel {
        private readonly ILogger<IndexModel> _logger;
        private readonly IFunesConnection _fconn;
        private readonly IDomainDeserializer _deserializer;

        public IFunesConnection FunesConnection => _fconn;
        public IDomainDeserializer Deserializer => _deserializer;

        public IndexModel(ILogger<IndexModel> logger, IFunesConnection fconn, IDomainDeserializer deserializerer) {
            _logger = logger;
            _fconn = fconn;
            _deserializer = deserializerer;
        }

        public void OnGet() {
        }
    }
}