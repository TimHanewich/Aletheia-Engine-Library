using System;
using SecuritiesExchangeCommission.Edgar;
using Aletheia;
using Newtonsoft.Json;

namespace testing
{
    class Program
    {
        static void Main(string[] args)
        {
            string content = System.IO.File.ReadAllText("C:\\Users\\tihanewi\\Downloads\\form4s\\amzn_long.xml");
            AletheiaProcessor ap = new AletheiaProcessor();
            SecurityTransaction[] transactions = ap.ProcessForm4(content, "999");
            Console.WriteLine(JsonConvert.SerializeObject(transactions));
        }
    }
}