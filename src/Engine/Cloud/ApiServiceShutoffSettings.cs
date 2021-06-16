using System;
using System.Collections.Generic;

namespace Aletheia.Engine.Cloud
{
    public class ApiServiceShutoffSettings
    {
        public bool Shutoff {get; set;}
        public string DenialMessage {get; set;} //This message will be returned to the users trying to use the API service when it is denying them.
        public ApiEndpointServiceShutoff[] EndpointShutoffs {get; set;} //Instead of shutoff the entire service, you can also block specific endpoints.
    
        public ApiServiceShutoffSettings()
        {
            Shutoff = false;
            DenialMessage = null;
            EndpointShutoffs = new List<ApiEndpointServiceShutoff>().ToArray();
        }

        public void AddEndpointShutoff(ApiEndpointServiceShutoff endpoint_shutoff)
        {
            //Make sure there isn't already one in there that has this ID
            foreach (ApiEndpointServiceShutoff shutoff in EndpointShutoffs)
            {
                if (shutoff.Endpoint == endpoint_shutoff.Endpoint)
                {
                    throw new Exception("An endpoint shutoff for endpoint '" + endpoint_shutoff + "' already exists.");
                }
            }

            List<ApiEndpointServiceShutoff> ToAddTo = new List<ApiEndpointServiceShutoff>();
            ToAddTo.AddRange(EndpointShutoffs);
            ToAddTo.Add(endpoint_shutoff);
            EndpointShutoffs = ToAddTo.ToArray();
        }
    
        public void RemoveEndpointShutoff(string endpoint_id)
        {
            List<ApiEndpointServiceShutoff> ToRemoveFrom = new List<ApiEndpointServiceShutoff>();
            ToRemoveFrom.AddRange(EndpointShutoffs);
            
            //Find the one to remove
            ApiEndpointServiceShutoff ToRemove = null;
            foreach (ApiEndpointServiceShutoff ess in ToRemoveFrom)
            {
                if (ess.Endpoint == endpoint_id)
                {
                    ToRemove = ess;
                }
            }

            if (ToRemove == null)
            {
                throw new Exception("Unable to find endpoint shutoff to remove with ID '" + endpoint_id + "'");
            }

            ToRemoveFrom.Remove(ToRemove); //Take it out
            EndpointShutoffs = ToRemoveFrom.ToArray(); //Set the new list
        }
    }
}