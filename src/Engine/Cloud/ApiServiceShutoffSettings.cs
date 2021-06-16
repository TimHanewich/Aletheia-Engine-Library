using System;

namespace Aletheia.Engine.Cloud
{
    public class ApiServiceShutoffSettings
    {
        public bool Shutoff {get; set;}
        public string DenialMessage {get; set;} //This message will be returned to the users trying to use the API service when it is denying them.
        public ApiEndpointServiceShutoff[] ShutoffEndpoints {get; set;} //Instead of shutoff the entire service, you can also block specific endpoints.
    }
}