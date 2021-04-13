using System;

namespace Aletheia.Service
{
    public class AletheiaService
    {
        public string ApiKey {get; set;}

        public AletheiaService(string api_key)
        {
            ApiKey = api_key;
        }
    }
}