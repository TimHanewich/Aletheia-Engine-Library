using System;
using SecuritiesExchangeCommission.Edgar;
using Aletheia;
using Newtonsoft.Json;
using System.Net;
using System.Net.Http;
using System.Collections.Generic;
using Aletheia.Cloud;
using Xbrl;
using System.IO;
using Xbrl.FinancialStatement;
using Aletheia.Fundamentals;
using TimHanewich.Investing;

namespace testing
{
    class Program
    {
        static void Main(string[] args)
        {
            
        }

        public static void TryOne()
        {
            Stream s = System.IO.File.Open("C:\\Users\\tihanewi\\Downloads\\AAPL_10K.xml", FileMode.Open);
            XbrlInstanceDocument doc = XbrlInstanceDocument.Create(s);
            FinancialStatement fs = doc.CreateFinancialStatement();

            AletheiaProcessor ap = new AletheiaProcessor();

            FundamentalsProcessingResult fpr = ap.ProcessFundamentals(fs);
            Console.WriteLine(JsonConvert.SerializeObject(fpr));
        }
    }
}