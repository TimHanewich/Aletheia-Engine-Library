using System;

namespace Aletheia.Cloud.User
{
    public class AletheiaApiCall
    {
        public DateTime CalledAtUtc {get; set;}
        public AletheiaApiKey ConsumedKey {get; set;}
        public string Endpoint {get; set;}
        public ApiCallDirection Direction {get; set;}
    }
}