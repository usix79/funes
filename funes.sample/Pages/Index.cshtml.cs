using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Extensions.Logging;

namespace funes.sample.Pages {
    public class IndexModel : PageModel {
        
        private readonly ILogger<IndexModel> _logger;

        [BindProperty]
        public string ConnectionType { get; set; }

        [BindProperty]
        public string FileSystemRoot { get; set; }

        [BindProperty]
        public string BucketName { get; set; }

        [BindProperty]
        public string RedisAddress { get; set; }

        [BindProperty] 
        public bool PopulateData { get; set; } = true;
        
        public string Error { get; set; }

        public IndexModel(ILogger<IndexModel> logger) {
            _logger = logger;
        }

        public void OnGet() {
            if (string.IsNullOrEmpty(FileSystemRoot))
                FileSystemRoot = App.Instance.CurrentFileSystemRoot;
            if (string.IsNullOrEmpty(BucketName))
                BucketName = App.Instance.CurrentBucket;
            if (string.IsNullOrEmpty(RedisAddress))
                RedisAddress = App.Instance.CurrentRedisAddress;
        }
        
        public async Task<IActionResult> OnPostAsync() {
            try {
                switch (ConnectionType) {
                    case "1":
                        App.Instance.InitializeInMemory();
                        break;
                    case "2":
                        App.Instance.InitializeFileSystem(FileSystemRoot);
                        break;
                    case "3":
                        App.Instance.InitializeAws(BucketName, RedisAddress);
                        break;
                    default:
                        throw new Exception($"Unknown App Type: {ConnectionType}");
                }

                if (PopulateData) {
                    var popResult = await App.Instance.PopulateSampleData();
                    if (popResult.IsError) throw new Exception(popResult.Error.ToString());
                }
                
                return RedirectToPage("./Books");
            }
            catch (Exception x) {
                Error = x.Message;
                return Page();    
            }
        }
    }
}