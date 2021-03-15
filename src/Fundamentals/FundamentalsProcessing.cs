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
                if (jp.Value.Type != JTokenType.Null)
                {
                    if (jp.Value.Type != JTokenType.Date)
                    {
                        FinancialFact ff = new FinancialFact();
                        ff.Id = Guid.NewGuid();
                        ff.ParentContext = context.Id;
                        ff.Value = Convert.ToSingle(jp.Value.ToString());

                        //Find the approprite label
                        int basic = -1;
                        FactLabel ThisLabel = (FactLabel)basic;
                        foreach (FactLabel fl in Enum.GetValues(typeof(FactLabel)))
                        {
                            if (fl.ToString() == jp.Name)
                            {
                                ThisLabel = fl;
                            }
                        }
                        ff.LabelId = ThisLabel;

                        FinancialFactsCol.Add(ff);
                    }
                }
            }

            ToReturn.FactContexts = FactContextsCol.ToArray();
            ToReturn.FinancialFacts = FinancialFactsCol.ToArray();
            return ToReturn;
        }
    }
}