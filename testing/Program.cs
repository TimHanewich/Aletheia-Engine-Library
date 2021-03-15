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
            string drop_in = "C:\\Users\\tihanewi\\Downloads\\FULL_COLLECTS";
            List<FundamentalsProcessingResult> Results = new List<FundamentalsProcessingResult>();
            AletheiaProcessor ap = new AletheiaProcessor();

            //<Make the dir if it doesnt exist
            if (System.IO.Directory.Exists(drop_in) == false)
            {
                System.IO.Directory.CreateDirectory(drop_in);
            }
            
            string[] sp500 = TimHanewich.Investing.InvestingToolkit.GetEquityGroupAsync(EquityGroup.SP500).Result;
            foreach (string s in sp500)
            {
                Console.WriteLine("Searching " + s.ToUpper() + "...");
                EdgarSearch es = EdgarSearch.CreateAsync(s, "10-K").Result;
                foreach (EdgarSearchResult esr in es.Results)
                {
                    Console.Write(s.ToUpper() + " --> " + esr.FilingDate.ToShortDateString());
                    if (esr.Filing == "10-K")
                    {
                        Stream docstream = null;
                        try
                        {
                            docstream = esr.DownloadXbrlDocumentAsync().Result;  
                        }
                        catch
                        {
                            Console.WriteLine("FAILURE while downloading stream.");
                            continue;
                        }

                        XbrlInstanceDocument doc = null;
                        try
                        {
                            doc = XbrlInstanceDocument.Create(docstream);  
                        }
                        catch
                        {
                            Console.WriteLine("FAILIURE while converting stream to instance document");
                            continue;
                        }

                        FinancialStatement fs = null;
                        try
                        {
                            fs = doc.CreateFinancialStatement();
                        }
                        catch
                        {
                            Console.WriteLine("FAILURE while converting to financial statement.");
                            continue;
                        }

                        //Process!
                        FundamentalsProcessingResult fpr = ap.ProcessFundamentals(fs);
                        Results.Add(fpr);

                        //Add to the drop in
                        string ToDropIn = JsonConvert.SerializeObject(Results.ToArray());
                        System.IO.File.WriteAllText(drop_in + "\\Total" + Results.Count.ToString() + ".json", ToDropIn);

                        Console.WriteLine(" Successful!");

                        
                    }
                }
            }

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