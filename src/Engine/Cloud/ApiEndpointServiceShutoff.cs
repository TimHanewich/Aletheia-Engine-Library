using System;
using Aletheia.Engine.Cloud.User;

namespace Aletheia.Engine.Cloud
{
    //This is used to shut off a specific endpoint
    public class ApiEndpointServiceShutoff
    {
        public AletheiaEndpoint Endpoint {get; set;} //The endpoint title (ID that is logged in the ApiCall)
        public string DenialMessage {get; set;} //The message that should be returned to the user when they attempt to call this endpoint but will be denied.
    }
}