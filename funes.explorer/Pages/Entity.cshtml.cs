using System;
using System.Globalization;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Funes.Explorer.Pages {
    public class EntityModel : PageModel {
        private readonly IFunesConnection _fconn;

        public EntityModel(IFunesConnection fconn) {
            _fconn = fconn;
        }

        [BindProperty(SupportsGet = true)]
        public string Id { get; set; }
        
        public EntityId Eid { get; private set; }
        
        [BindProperty(SupportsGet = true)]
        public string BeforeId { get; set; }

        [BindProperty(SupportsGet = true)]
        public string BeforeTimestamp { get; set; }
        
        public string Error { get; set; }

        public IncrementId[] Items { get; private set; }
        
        public async Task OnGetAsync() {
            Eid = new EntityId(Id);
            var incId = IncrementId.Singularity;

            if (DateTimeOffset.TryParseExact(
                BeforeTimestamp,
                "yyyy-MM-dd HH:mm:ss fff",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal,
                out var dt)) {
                incId = IncrementId.ComposeId(dt, "");
            }
            else {
                if (!string.IsNullOrEmpty(BeforeId)) {
                    incId = new IncrementId(BeforeId);
                }
            }

            var historyResult = await _fconn.Repo.HistoryBefore(Eid, incId, 100);
            if (historyResult.IsError) {
                Error = historyResult.Error.ToString();
                return;
            }

            Items = historyResult.Value;
        }
    }
}