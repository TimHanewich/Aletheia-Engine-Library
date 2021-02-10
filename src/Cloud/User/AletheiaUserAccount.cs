using System;

namespace Aletheia.Cloud.User
{
    public class AletheiaUserAccount
    {
        public string Username {get; set;}
        public string Password {get; set;}
        public string Email {get; set;}
        public DateTime CreatedAtUtc {get; set;}
    }
}