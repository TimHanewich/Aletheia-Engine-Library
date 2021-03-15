using System;
using Xbrl;
using Xbrl.FinancialStatement;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Aletheia.Fundamentals
{
    public static class FundamentalsProcessing
    {
        public static FundamentalsProcessingResult ProcessFundamentals(this AletheiaProcessor ap, FinancialStatement fs)
        {
            FundamentalsProcessingResult ToReturn = new FundamentalsProcessingResult();
            List<FactContext> FactContextsCol = new List<FactContext>();
            List<FinancialFact> FinancialFactsCol = new List<FinancialFact>();


            //Add the fact context
            FactContext context = new FactContext();
            context.Id = Guid.NewGuid();
            if (fs.PeriodStart.HasValue)
            {
                context.Start = fs.PeriodStart.Value;
            }
            else
            {
                context.Start = null;
            }
            if (fs.PeriodEnd.HasValue)
            {
                context.End = fs.PeriodEnd.Value;
            }
            FactContextsCol.Add(context);


            //Add each property
            JObject jo = JObject.Parse(JsonConvert.SerializeObject(fs));
            foreach (JProperty jp in jo.Properties())
            {
                if (jp.Value.Type != JTokenType.Date)
                {
                    FinancialFact ff = new FinancialFact();
                    ff.Id = Guid.NewGuid();
                    ff.ParentContext = context.Id;
                    ff.Label = jp.Name;
                    //Console.WriteLine("Val type: " + jp.Value.Type.ToString() + ". Val: " + jp.Value.ToString());
                    ff.Value = Convert.ToSingle(jp.Value.ToString());
                    FinancialFactsCol.Add(ff);
                }
            }

            ToReturn.FactContexts = FactContextsCol.ToArray();
            ToReturn.FinancialFacts = FinancialFactsCol.ToArray();
            return ToReturn;
        }
    }
}