using System;

namespace Aletheia.Engine.Cloud.User
{
    public class AletheiaApiKey
    {
        public Guid Token {get; set;}
        public AletheiaUserAccount RegisteredTo {get; set;}
        public DateTime CreatedAtUtc {get; set;}
    }
}