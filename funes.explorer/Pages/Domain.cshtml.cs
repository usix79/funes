using System.Linq;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Funes.Explorer.Pages {
    public class DomainModel : PageModel {

        private readonly IFunesConnection _fconn;

        public DomainModel(IFunesConnection fconn) {
            _fconn = fconn;
        }

        [BindProperty(SupportsGet = true)]
        public string Category { get; set; }

        [BindProperty(SupportsGet = true)]
        public string After { get; set; }

        public string Error { get; private set; }
        
        public string[] Items { get; private set; }
        
        public async Task<IActionResult> OnGetAsync() {
            var listRes = await _fconn.Repo.List(Category ?? "", After ?? "", Constants.ItemsOnPage);
            if (listRes.IsError) {
                Error = listRes.Error.ToString();
                return new PageResult();
            }

            if (!string.IsNullOrEmpty(Category) && string.IsNullOrEmpty(After) && listRes.Value.Length == 0) {
                return RedirectToPage("/Entity", new {Id = Category});
            }
            
            Items = listRes.Value;
            if (string.IsNullOrEmpty(Category)) {
                // do not show system directory
                Items = Items.Where(x => x != EntityId.SystemCategory).ToArray();
            }
            return new PageResult();
        }
    }
}