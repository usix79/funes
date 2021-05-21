using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Funes.Indexes;
using funes.sample.Domain;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

namespace funes.sample.Pages {
    public class PrivacyModel : PageModel {
        
        private readonly ILogger<PrivacyModel> _logger;

        [BindProperty(SupportsGet = true)]
        public string NameFrom { get; set; }

        [BindProperty(SupportsGet = true)]
        public string NameTo { get; set; }
        
        public string Error { get; set; }

        public Book[] Books { get; set; } = Array.Empty<Book>();

        public PrivacyModel(ILogger<PrivacyModel> logger) {
            _logger = logger;
        }

        public async Task OnGetAsync() {
            if (!App.Instance.IsInitialized) {
                Error = "Not Initialized";
                return;
            }

            var ctx = App.Instance.CrateContext();

            var selectResult = await IndexesModule.SelectAsc(ctx, default, Helpers.Constants.IdxBooks,
                NameFrom, NameTo != "" ? NameTo : null, "", 1000);

            if (selectResult.IsError) {
                Error = selectResult.Error.ToString();
                return;
            }

            var books = new List<Book>();
            foreach (var pair in selectResult.Value.Pairs) {
                var eid = Helpers.CreateBookEntityId(pair.Key);
                var retResult = await ctx.Retrieve(eid, default);
                if (retResult.IsError) {
                    Error = retResult.Error.ToString();
                    return;
                }

                if (ctx.TryGetEntry(eid, false, out var entry)) {
                    if (entry.IsOk) {
                        books.Add((Book)entry.Value);
                    }
                }
            }

            Books = books.ToArray();
        }

        public async Task<JsonResult> OnGetLikeAsync(string bookId) {
            var likeResult = await App.Instance.LikeBook(bookId);
            if (likeResult.IsError) throw new Exception(likeResult.Error.ToString());

            var ctx = App.Instance.CrateContext();
            var eid = Helpers.CreateBookEntityId(bookId);
            var retResult = await ctx.Retrieve(eid, default);
            if (retResult.IsError) throw new Exception(retResult.Error.ToString());

            if (ctx.TryGetEntry(eid, false, out var entry)) {
                if (entry.IsOk) {
                    return new JsonResult(entry.Value);
                }
            }
            
            throw new Exception("retrieve error");            
        }
    }
}