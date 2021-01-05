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
            // List<SecurityTransaction> SecurityTransactions = new List<SecurityTransaction>();
            // AletheiaProcessor ap = new AletheiaProcessor();
            // HttpClient hc = new HttpClient();
            // EdgarSearch es = EdgarSearch.CreateAsync(args[0], "4", null, EdgarSearchOwnershipFilter.only).Result;
            // foreach (EdgarSearchResult esr in es.Results)
            // {
            //     if (esr.Filing == "4")
            //     {
            //         FilingDocument[] docs = esr.GetDocumentFormatFilesAsync().Result;
            //         foreach (FilingDocument fd in docs)
            //         {
            //             if (fd.DocumentName.ToLower().Contains(".xml"))
            //             {
            //                 Console.WriteLine("Processing " + fd.Url + "...");
            //                 HttpResponseMessage hrm = hc.GetAsync(fd.Url).Result;
            //                 string content = hrm.Content.ReadAsStringAsync().Result;
            //                 StatementOfChangesInBeneficialOwnership form4 = StatementOfChangesInBeneficialOwnership.ParseXml(content);
            //                 SecurityTransaction[] trans = ap.ProcessForm4(content, esr.GetAccessionNumberAsync().Result);
            //                 SecurityTransactions.AddRange(trans);
            //             }
            //         }
            //     }
            // }     
        }
    }
}