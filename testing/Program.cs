using System;
using SecuritiesExchangeCommission.Edgar;
using Aletheia;
using Newtonsoft.Json;
using System.Net;
using System.Net.Http;
using System.Collections.Generic;
using Aletheia.Cloud;

namespace testing
{
    class Program
    {
        static void Main(string[] args)
        {
            AletheiaCredentialPackage cp = new AletheiaCredentialPackage();
            cp.AzureStorageConnectionString = "hello_world";
            cp.SqlConnectionString = "";
        
            AletheiaCloudClient acc = new AletheiaCloudClient(cp);
            
            AletheiaProcessor ap = new AletheiaProcessor();
            AletheiaProcessingResult apr = ap.ProcessForm4Async("https://www.sec.gov/Archives/edgar/data/789019/000106299321001415/0001062993-21-001415-index.htm").Result;

            foreach (SecurityTransactionHolding sth in apr.SecurityTransactionHoldings)
            {
                acc.UploadSecurityTransactionHoldingAsync(sth).Wait();
            }
        }
    }
}