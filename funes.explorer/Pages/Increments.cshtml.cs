using System;
using System.Globalization;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Funes.Explorer.Pages {
    public class IncrementsModel : PageModel {

        private readonly IFunesConnection _fconn;

        public IncrementsModel(IFunesConnection fconn) {
            _fconn = fconn;
        }

        [BindProperty(SupportsGet = true)]
        public string IdValue { get; set; }

        [BindProperty(SupportsGet = true)]
        public string BeforeId { get; set; }

        [BindProperty(SupportsGet = true)]
        public string BeforeTimestamp { get; set; }
        
        public string Error { get; set; }

        public IncrementId[] Items { get; private set; }
        
        public Increment Item { get; private set; }

        public Task OnGetAsync() =>
            string.IsNullOrEmpty(IdValue) ? LoadHistory() : LoadIncrement(); 
        
        public async Task LoadHistory() {
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

            var historyResult = await _fconn.Repo.HistoryBefore(Funes.Increment.GlobalIncrementId, incId, Constants.ItemsOnPage);
            if (historyResult.IsError) {
                Error = historyResult.Error.ToString();
                return;
            }

            Items = historyResult.Value;
        }

        public async Task LoadIncrement() {
            var key = Increment.CreateStampKey(new IncrementId(IdValue));
            var loadResult = await _fconn.Repo.Load(key, default);
            if (loadResult.IsError) {
                Error = loadResult.Error.ToString();
                return;
            }

            var decodeResult = Increment.Decode(loadResult.Value.Data);
            if (decodeResult.IsError) {
                Error = decodeResult.Error.ToString();
                return;
            }

            Item = decodeResult.Value;
        }
    }
}