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
            //The method I am using right here for going from the FinancialStatement to FinancialFact's is a lazy shortcut for right now.
            //The only reason this works is if the FactLabels (the enum) has a direct 1:1 exact match to the FinancialStatement properties in the Xbrl class.
            //This is because this method matches the property name (from the Financial Statement) to the enum string representation values and uses that match as the label ID #
            //If there was NOT a match, this would not work.
            //In the future i should replace this with individually doing this for each propety in the Financial Statement.
            //Or at least specify the property names which this should be done for.
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
                    
                        

                        //Should we add it? Only if it did find a 1:1 label to propert name match
                        int ThisLabelNum = Convert.ToInt32(ff.LabelId);
                        if (ThisLabelNum != -1)
                        {
                            FinancialFactsCol.Add(ff);
                        }
                    }
                }
            }

            ToReturn.FactContexts = FactContextsCol.ToArray();
            ToReturn.FinancialFacts = FinancialFactsCol.ToArray();
            return ToReturn;
        }
    }
}