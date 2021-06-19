using System;

namespace Aletheia.Engine.Cloud.User
{
    public class AletheiaApiCall
    {
        public Guid Id {get; set;}
        public DateTime CalledAtUtc {get; set;}
        public Guid ConsumedKey {get; set;}
        public AletheiaApiKey _ConsumedKey {get; set;}
        public AletheiaEndpoint Endpoint {get; set;}
        public ApiCallDirection Direction {get; set;}
        public float? ResponseTime {get; set;} //The time it took to fulfill the API call in seconds.
    }
}