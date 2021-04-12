using System;

namespace Aletheia.Engine.Cloud.User
{
    public class AletheiaApiCall
    {
        public DateTime CalledAtUtc {get; set;}
        public Guid ConsumedKey {get; set;}
        public AletheiaApiKey _ConsumedKey {get; set;}
        public string Endpoint {get; set;}
        public ApiCallDirection Direction {get; set;}
    }
}