using System;

namespace Aletheia.Cloud
{
    public class AletheiaCredentialPackage
    {
        public string SqlConnectionString {get; set;}
        public string AzureStorageConnectionString {get; set;}
        public string SendGridKey {get; set;}
    }
}