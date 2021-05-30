using System;

namespace Aletheia.Engine.Cloud
{
    public class AletheiaCredentialPackage
    {
        public string SqlConnectionString {get; set;}
        public string AzureStorageConnectionString {get; set;}
        
        public AletheiaCredentialPackage()
        {
            
        }

        public AletheiaCredentialPackage(string sql_connection_string, string az_storage_string)
        {
            SqlConnectionString = sql_connection_string;
            AzureStorageConnectionString = az_storage_string;
        }
    }
}