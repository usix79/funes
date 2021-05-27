using System.Text.Json;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Funes.Explorer.Pages {
    public class EntityStampModel : PageModel {
        
        private readonly IFunesConnection _fconn;
        private readonly IDomainDeserializer _deserializer;

        public EntityStampModel(IFunesConnection fconn, IDomainDeserializer deserializer) {
            _fconn = fconn;
            _deserializer = deserializer;
        }

        [BindProperty(SupportsGet = true)]
        public string Key { get; set; }
        
        public EntityId Eid { get; private set; }
        
        public IncrementId IncId { get; private set; }
        
        public BinaryData Data { get; private set; }
        
        public string ContentAsText { get; private set; }
        
        public string Error { get; set; }
        
        
        public async Task OnGetAsync() {

            var idx = Key.LastIndexOf('/');
            Eid = new EntityId(Key.Substring(0, idx));
            IncId = new IncrementId(Key.Substring(idx + 1));


            var retResult = await _fconn.Repo.Load(new StampKey(Eid, IncId), default);
            if (retResult.IsError) {
                Error = retResult.Error.ToString();
                return;
            }

            Data = retResult.Value.Data;

            var deserResult = _deserializer.Deserialize(Eid, Data);
            if (deserResult.IsError) {
                ContentAsText = deserResult.Error.ToString();
            }
            else {
                var opt = new JsonSerializerOptions {WriteIndented = true};
                ContentAsText = JsonSerializer.Serialize(deserResult.Value, opt);
            }
        }
    }
}