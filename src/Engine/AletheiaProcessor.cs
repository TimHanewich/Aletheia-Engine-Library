using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Aletheia.InsiderTrading;
using System.IO;
using Aletheia.Engine.EarningsCalls;
using TheMotleyFool.Transcripts;
using Aletheia.Engine.EarningsCalls.ProcessingComponents;

namespace Aletheia.Engine
{
    public class AletheiaProcessor
    {     
        
        public async Task<AletheiaProcessingResult> ProcessStatementOfBeneficialOwnershipAsync(string filing_url)
        {
            EdgarFiling ef = new EdgarFiling();
            ef.DocumentsUrl = filing_url;
            EdgarFilingDetails details = await ef.GetFilingDetailsAsync();
            foreach (FilingDocument fd in details.DocumentFormatFiles)
            {
                if (fd.DocumentName.Trim().ToLower().Contains(".xml"))
                {
                    SecRequestManager reqmgr = new SecRequestManager();
                    Stream s = await reqmgr.SecGetStreamAsync(fd.Url);
                    StreamReader sr = new StreamReader(s);
                    string content = await sr.ReadToEndAsync();
                    string accession_num = details.AccessionNumberP1.ToString() + "-" + details.AccessionNumberP2.ToString() + "-" + details.AccessionNumberP3.ToString();
                    AletheiaProcessingResult apr = ProcessStatementOfBeneficialOwnership(content, accession_num, filing_url);
                    return apr;
                }
            }
            throw new Exception("Unable to find data document in the supplied filing."); //If it got this far it didnt find the proper filing
        }

        public AletheiaProcessingResult ProcessStatementOfBeneficialOwnership(string xml, string sec_accession_num, string filing_url)
        {
            //Create the form4
            StatementOfBeneficialOwnership form = StatementOfBeneficialOwnership.ParseXml(xml);
            return ProcessStatementOfBeneficialOwnership(form, sec_accession_num, filing_url);
        }
    
        public AletheiaProcessingResult ProcessStatementOfBeneficialOwnership(StatementOfBeneficialOwnership form, string sec_accession_num, string filing_url)
        {
            AletheiaProcessingResult ToReturn = new AletheiaProcessingResult();
            List<SecEntity> ToAppend_SecEntities = new List<SecEntity>();
            List<SecFiling> ToAppend_SecFilings = new List<SecFiling>();
            List<HeldOfficerPosition> ToAppend_HeldOfficerPositions = new List<HeldOfficerPosition>();
            List<SecurityTransactionHolding> ToAppend_SecurityTransactionHoldings = new List<SecurityTransactionHolding>();

            //Get issuer entity
            SecEntity issuer = new SecEntity();
            issuer.Cik = Convert.ToInt64(form.IssuerCik);
            issuer.Name = form.IssuerName.Trim();
            issuer.TradingSymbol = form.IssuerTradingSymbol.Trim().ToUpper();
            ToAppend_SecEntities.Add(issuer);

            //Get owner entity
            SecEntity owner = new SecEntity();
            owner.Cik = Convert.ToInt64(form.OwnerCik);
            owner.Name = form.OwnerName.Trim();
            ToAppend_SecEntities.Add(owner);

            //Do the SEC filing  
            List<string> Splitter = new List<string>();
            Splitter.Add("-");
            string[] AccessionParts = sec_accession_num.Split(Splitter.ToArray(), StringSplitOptions.None);      
            SecFiling filing = new SecFiling();
            filing.Id = Guid.NewGuid();
            filing.FilingUrl = filing_url;
            filing.AccessionP1 = Convert.ToInt64(AccessionParts[0]);
            filing.AccessionP2 = Convert.ToInt32(AccessionParts[1]);
            filing.AccessionP3 = Convert.ToInt32(AccessionParts[2]);
            filing.FilingType = FilingType.Form4;
            filing.ReportedOn = form.PeriodOfReport;
            filing.Issuer = issuer.Cik;
            filing.Owner = owner.Cik;
            ToAppend_SecFilings.Add(filing);


            //Now do each transaction holding - non-derivative first
            if (form.NonDerivativeTransactions != null)
            {
                foreach (NonDerivativeTransaction ndt in form.NonDerivativeTransactions)
                {
                    SecurityTransactionHolding sth = new SecurityTransactionHolding();
                    sth.Id = Guid.NewGuid();
                    sth.FromFiling = filing.Id;
                    if (ndt.IsHolding()) //It is a holding
                    {
                        sth.EntryType = TransactionHoldingEntryType.Holding;
                    }
                    else //It is a transaction 
                    {

                        //Since it is a transaction, this bracket will enter in all the transaction properties

                        sth.EntryType = TransactionHoldingEntryType.Transaction;

                        //Acquired or disposed
                        if (ndt.AcquiredOrDisposed == SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Acquired)
                        {
                            sth.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Acquired;
                        }
                        else if (ndt.AcquiredOrDisposed ==  SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Disposed)
                        {
                            sth.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Disposed;
                        }

                        //Quantity (transaction related)
                        if (ndt.TransactionQuantity.HasValue)
                        {
                            sth.Quantity = ndt.TransactionQuantity.Value;
                        }
                        
                        //Price per security
                        if (ndt.TransactionPricePerSecurity.HasValue)
                        {
                            sth.PricePerSecurity = ndt.TransactionPricePerSecurity.Value;
                        }
                        
                        //Transaction Date
                        if (ndt.TransactionDate.HasValue)
                        {
                            sth.TransactionDate = ndt.TransactionDate.Value;
                        }
                    
                        //Transaction Code
                        if (ndt.TransactionCode.HasValue)
                        {
                            sth.TransactionCode = ndt.TransactionCode.Value;
                        }
                    }

                    //Quantity owned following transaction
                    sth.QuantityOwnedFollowingTransaction = ndt.SecuritiesOwnedFollowingTransaction;

                    //Direct or indirect ownership?
                    if (ndt.DirectOrIndirectOwnership == OwnershipNature.Direct)
                    {
                        sth.DirectIndirect = DirectIndirect.Direct;
                    }
                    else if (ndt.DirectOrIndirectOwnership == OwnershipNature.Indirect)
                    {
                        sth.DirectIndirect = DirectIndirect.Indirect;
                    }

                    //Security Title
                    sth.SecurityTitle = ndt.SecurityTitle;

                    //Security type
                    sth.SecurityType = SecurityType.NonDerivative;

                    ToAppend_SecurityTransactionHoldings.Add(sth);                
                }
            }
            
            //Now do each transaction holding - derivative
            if (form.DerivativeTransactions != null)
            {
                foreach (DerivativeTransaction dt in form.DerivativeTransactions)
                {
                    SecurityTransactionHolding sth = new SecurityTransactionHolding();
                    sth.Id = Guid.NewGuid();
                    sth.FromFiling = filing.Id;
                    if (dt.IsHolding()) //It is a holding
                    {
                        sth.EntryType = TransactionHoldingEntryType.Holding;
                    }
                    else //It is a transaction 
                    {

                        //Since it is a transaction, this bracket will enter in all the transaction properties

                        sth.EntryType = TransactionHoldingEntryType.Transaction;

                        //Acquired or disposed
                        if (dt.AcquiredOrDisposed == SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Acquired)
                        {
                            sth.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Acquired;
                        }
                        else if (dt.AcquiredOrDisposed ==  SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Disposed)
                        {
                            sth.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Disposed;
                        }

                        //Quantity (transaction related)
                        if (dt.TransactionQuantity.HasValue)
                        {
                            sth.Quantity = dt.TransactionQuantity.Value;
                        }
                        
                        //Price per security
                        if (dt.TransactionPricePerSecurity.HasValue)
                        {
                            sth.PricePerSecurity = dt.TransactionPricePerSecurity.Value;
                        }
                        
                        //Transaction Date
                        if (dt.TransactionCode.HasValue)
                        {
                            sth.TransactionDate = dt.TransactionDate.Value;
                        }
                        
                        //Transaction Code
                        if (sth.TransactionCode.HasValue)
                        {
                            sth.TransactionCode = dt.TransactionCode.Value;
                        }  
                    }

                    //Quantity owned following transaction
                    sth.QuantityOwnedFollowingTransaction = dt.SecuritiesOwnedFollowingTransaction;

                    //Direct or indirect ownership?
                    if (dt.DirectOrIndirectOwnership == OwnershipNature.Direct)
                    {
                        sth.DirectIndirect = DirectIndirect.Direct;
                    }
                    else if (dt.DirectOrIndirectOwnership == OwnershipNature.Indirect)
                    {
                        sth.DirectIndirect = DirectIndirect.Indirect;
                    }

                    //Security Title
                    sth.SecurityTitle = dt.SecurityTitle;

                    //Security type
                    sth.SecurityType = SecurityType.Derivative;

                    //If the security type is non-derivative (it should be in this case every time because that is the type of stuff we are looping through right now), apply those properties

                    //Conversion or excercise price
                    if (dt.ConversionOrExercisePrice.HasValue)
                    {
                        sth.ConversionOrExercisePrice = dt.ConversionOrExercisePrice.Value;
                    }
                    
                    //Excercisable date
                    if (dt.Exercisable.HasValue)
                    {
                        sth.ExercisableDate = dt.Exercisable.Value;
                    }

                    //Expiration date
                    if (dt.Expiration.HasValue)
                    {
                        sth.ExpirationDate = dt.Expiration.Value;
                    }

                    //Underlying security title
                    if (dt.UnderlyingSecurityTitle != null)
                    {
                        sth.UnderlyingSecurityTitle = dt.UnderlyingSecurityTitle;
                    }

                    //Underlying security quantity
                    sth.UnderlyingSecurityQuantity = dt.UnderlyingSecurityQuantity;

                    ToAppend_SecurityTransactionHoldings.Add(sth);                
                }
            }
            
            //Officer held positions?
            if (form.OwnerIsOfficer)
            {
                if (form.OwnerOfficerTitle != null)
                {
                    if (form.OwnerOfficerTitle != "")
                    {
                        HeldOfficerPosition hop = new HeldOfficerPosition();
                        hop.Id = Guid.NewGuid();
                        hop.Officer = owner.Cik;
                        hop.Company = issuer.Cik;
                        hop.PositionTitle = form.OwnerOfficerTitle;
                        hop.ObservedOn = filing.Id;
                        ToAppend_HeldOfficerPositions.Add(hop);
                    }
                }
            }

            //Append and return
            ToReturn.SecEntities = ToAppend_SecEntities.ToArray();
            ToReturn.SecFilings = ToAppend_SecFilings.ToArray();
            ToReturn.HeldOfficerPositions = ToAppend_HeldOfficerPositions.ToArray();
            ToReturn.SecurityTransactionHoldings = ToAppend_SecurityTransactionHoldings.ToArray();
            return ToReturn;
        }

        public async Task<AletheiaEarningsCallProcessingResult> ProcessEarningsCallAsync(string tmf_transcript_url)
        {
            Transcript t = await Transcript.CreateFromUrlAsync(tmf_transcript_url);
            AletheiaEarningsCallProcessingResult ToReturn = ProcessEarningsCall(t, tmf_transcript_url);
            return ToReturn;
        }

        public AletheiaEarningsCallProcessingResult ProcessEarningsCall(Transcript trans)
        {
            //Create the item to return
            AletheiaEarningsCallProcessingResult ToReturn = new AletheiaEarningsCallProcessingResult();
            List<CallCompany> CallCompanies = new List<CallCompany>();
            List<EarningsCall> EarningsCalls = new List<EarningsCall>();
            List<SpokenRemark> SpokenRemarks = new List<SpokenRemark>();
            List<Aletheia.Engine.EarningsCalls.CallParticipant> CallParticipants = new List<Aletheia.Engine.EarningsCalls.CallParticipant>();
            List<SpokenRemarkHighlight> SpokenRemarkHighlights = new List<SpokenRemarkHighlight>();

            int loc1 = 0;
            int loc2 = 0;

            #region "Get company"

            CallCompany cc = new CallCompany();
            cc.Id = Guid.NewGuid();
            
            //Get the company name from the title
            loc2 = trans.Title.LastIndexOf(")");
            if (loc2 != -1)
            {
                loc1 = trans.Title.LastIndexOf("(", loc2);
                if (loc1 != -1 && loc1 > 0)
                {
                    if (loc2 > loc1)
                    {
                        cc.Name = trans.Title.Substring(0, loc1 - 1).Trim();
                        cc.Name = StripOfUnsafeCharacters(cc.Name);
                    }
                    else
                    {
                        cc.Name = null;
                    }
                }
            }
            else //If paranthesis don't exist, try to get it from the quarter
            {
                //Get the posiiton of the quarter
                loc1 = -1;
                if (loc1 == -1)
                {
                    loc1 = trans.Title.IndexOf(" Q1");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.IndexOf(" Q2");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.IndexOf(" Q3");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.IndexOf(" Q4");
                }


                if (loc1 != -1)
                {
                    cc.Name = trans.Title.Substring(0, loc1).Trim();
                }
            }
            

            //Get the trading symbol
            cc.TradingSymbol = trans.TradingSymbol;

            CallCompanies.Add(cc);

            #endregion

            #region "Get the EarningsCall"

            EarningsCall ec = new EarningsCall();
            ec.Id = Guid.NewGuid();
            ec.ForCompany = cc.Id;
            ec.Url = null;
            ec.Title = StripOfUnsafeCharacters(trans.Title);

            //Get the Quarter (fiscal period)
            loc1 = trans.Title.LastIndexOf(")");
            if (loc1 != -1)
            {
                loc1 = trans.Title.IndexOf(" ", loc1);
                if (loc1 != -1)
                {
                    loc2 = trans.Title.IndexOf(" ", loc1 + 1);
                    if (loc2 > loc1)
                    {
                        string quartertxt = trans.Title.Substring(loc1 + 1, loc2 - loc1 - 1).Trim().ToLower();
                        if (quartertxt == "q1")
                        {
                            ec.Period = FiscalPeriod.Q1;
                        }
                        else if (quartertxt == "q2")
                        {
                            ec.Period = FiscalPeriod.Q2;
                        }
                        else if (quartertxt == "q3")
                        {
                            ec.Period = FiscalPeriod.Q3;
                        }
                        else if (quartertxt == "q4")
                        {
                            ec.Period = FiscalPeriod.Q4;
                        }
                    }
                }
            }
            else //If paranthesis were not used in the title, find it another way
            {
                if (trans.Title.ToLower().Contains(" q1 "))
                {
                    ec.Period = FiscalPeriod.Q1;
                }
                else if (trans.Title.ToLower().Contains(" q2 "))
                {
                    ec.Period = FiscalPeriod.Q2;
                }
                else if (trans.Title.ToLower().Contains(" q3 "))
                {
                    ec.Period = FiscalPeriod.Q3;
                }
                else if (trans.Title.ToLower().Contains(" q4 "))
                {
                    ec.Period = FiscalPeriod.Q4;
                }
            }
            

            //Get the year
            loc1 = trans.Title.LastIndexOf(")");
            if (loc1 != -1)
            {
                loc1 = trans.Title.IndexOf(" ", loc1);
                if (loc1 != -1)
                {
                    loc1 = trans.Title.IndexOf(" ", loc1 + 1);
                    if (loc1 != -1)
                    {
                        loc2 = trans.Title.IndexOf(" ", loc1 + 1);
                        if (loc2 > loc1)
                        {
                            string yeartxt = trans.Title.Substring(loc1 + 1, loc2 - loc1 - 1);
                            try
                            {
                                ec.Year = Convert.ToInt32(yeartxt);
                            }
                            catch
                            {

                            }
                        }
                    }
                }
            }
            else //If the title did not have paranthesis in it
            {
                loc1 = -1;
                if (loc1 == -1)
                {
                    loc1 = trans.Title.ToLower().IndexOf(" q1 ");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.ToLower().IndexOf(" q2 ");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.ToLower().IndexOf(" q3 ");
                }
                if (loc1 == -1)
                {
                    loc1 = trans.Title.ToLower().IndexOf(" q4 ");
                }

                if (loc1 != -1)
                {
                    loc1 = trans.Title.IndexOf(" ", loc1 + 1);
                    if (loc1 != -1)
                    {
                        loc2 = trans.Title.IndexOf(" ", loc1 + 1);
                        if (loc2 > loc1)
                        {
                            string yearstr = trans.Title.Substring(loc1 + 1, loc2 - loc1 - 1);
                            try
                            {
                                ec.Year = Convert.ToInt32(yearstr);
                            }
                            catch
                            {

                            }
                        }
                    }
                }
            }
            

            //Held At
            ec.HeldAt = trans.CallDateTimeStamp;

            //Add it!
            EarningsCalls.Add(ec);

            #endregion

            #region "Get call participants"

            foreach (TheMotleyFool.Transcripts.CallParticipant cp in trans.Participants)
            {
                Aletheia.Engine.EarningsCalls.CallParticipant ncp = new Aletheia.Engine.EarningsCalls.CallParticipant();
                ncp.Id = Guid.NewGuid();
                ncp.Name = StripOfUnsafeCharacters(cp.Name);
                ncp.Title = StripOfUnsafeCharacters(cp.Title);
                if (cp.Title == null)
                {
                    ncp.IsExternal = false;
                }
                else
                {
                    if (cp.Title.Contains("--"))
                    {
                        ncp.IsExternal = true;
                    }
                    else
                    {
                        ncp.IsExternal = false;
                    }
                }
                
                CallParticipants.Add(ncp);
            }

            #endregion

            #region "Get spoken remarks"

            int SeqNum = 0; //Sequence number

            foreach (Remark r in trans.Remarks)
            {
                foreach (string rs in r.SpokenRemarks)
                {
                    SpokenRemark sr = new SpokenRemark();
                    sr.Id = Guid.NewGuid();
                    sr.FromCall = ec.Id;
                    sr.SequenceNumber = SeqNum;

                    //Find the speaker ID
                    foreach (Aletheia.Engine.EarningsCalls.CallParticipant cp in CallParticipants)
                    {
                        if (cp.Name == r.Speaker.Name && cp.Title == r.Speaker.Title) //If the name and title match
                        {
                            sr.SpokenBy = cp.Id;
                        }
                    }

                    //Store the remark
                    sr.Remark = rs;

                    //Strip any new lines out of the remark
                    sr.Remark = sr.Remark.Replace("\n", " ");
                    sr.Remark = sr.Remark.Replace(Environment.NewLine, " ");
                    sr.Remark = sr.Remark.Replace("\r", " ");
                    sr.Remark = sr.Remark.Replace("\n\r", " ");
                    sr.Remark = sr.Remark.Replace("\r\n", "");

                    SpokenRemarks.Add(sr);

                    //Incremented the sequence number
                    SeqNum = SeqNum + 1;
                }   
            }

            #endregion

            #region "Get spoken remark highlights"

            //Prepare the keywords
            List<RemarkKeyword> Keywords = new List<RemarkKeyword>();
            Keywords.Add(new RemarkKeyword("revenue", HighlightCategory.Revenue, 3f));
            Keywords.Add(new RemarkKeyword("net income", HighlightCategory.Earnings, 3f));
            Keywords.Add(new RemarkKeyword("earnings per share", HighlightCategory.Earnings, 4f));
            Keywords.Add(new RemarkKeyword("record", HighlightCategory.Growth, 2f));
            Keywords.Add(new RemarkKeyword("growth", HighlightCategory.Growth, 3f));
            Keywords.Add(new RemarkKeyword("$", HighlightCategory.FinancialFigure, 4f));
            Keywords.Add(new RemarkKeyword("%", HighlightCategory.FinancialFigure, 4f));
            Keywords.Add(new RemarkKeyword("revenue grew", HighlightCategory.Revenue, 8f));
            Keywords.Add(new RemarkKeyword("revenue fell", HighlightCategory.Revenue, 8f));
            Keywords.Add(new RemarkKeyword("income grew", HighlightCategory.Earnings, 8f));
            Keywords.Add(new RemarkKeyword("income fell", HighlightCategory.Earnings, 8f));
            Keywords.Add(new RemarkKeyword("increase in volume", HighlightCategory.Volume, 2f));
            Keywords.Add(new RemarkKeyword("decrease in volume", HighlightCategory.Volume, 2f));
            Keywords.Add(new RemarkKeyword("brought down our cost", HighlightCategory.Earnings, 4f));
            Keywords.Add(new RemarkKeyword("cash flow", HighlightCategory.CashFlow, 5f));
            Keywords.Add(new RemarkKeyword("net profit", HighlightCategory.Earnings, 5f));
            Keywords.Add(new RemarkKeyword("net loss", HighlightCategory.Earnings, 5f));
            Keywords.Add(new RemarkKeyword("cash flow", HighlightCategory.CashFlow, 3f));
            Keywords.Add(new RemarkKeyword("per share", HighlightCategory.Earnings, 3f));
            Keywords.Add(new RemarkKeyword("we think", HighlightCategory.Guidance, 1f));
            Keywords.Add(new RemarkKeyword("surprised", HighlightCategory.ManagementPerception, 2f));
            Keywords.Add(new RemarkKeyword("surprising", HighlightCategory.ManagementPerception, 2f));
            Keywords.Add(new RemarkKeyword("we generated revenue", HighlightCategory.Revenue, 7f));
            Keywords.Add(new RemarkKeyword("guidance", HighlightCategory.Guidance, 6f));
            Keywords.Add(new RemarkKeyword("surpassed", HighlightCategory.Growth, 3f));
            Keywords.Add(new RemarkKeyword("top selling", HighlightCategory.Revenue, 2f));
            Keywords.Add(new RemarkKeyword("record performance", HighlightCategory.Growth, 1f));
            Keywords.Add(new RemarkKeyword("revenue reached", HighlightCategory.Revenue, 7f));
            Keywords.Add(new RemarkKeyword("historic", HighlightCategory.Growth, 2f));
            Keywords.Add(new RemarkKeyword("extraordinary", HighlightCategory.Growth, 1f));

            //Find highlights
            foreach (SpokenRemark sr in SpokenRemarks)
            {
                foreach (RemarkKeyword rk in Keywords)
                {
                    int[] appearances = AllIndexOf(sr.Remark, rk.KeywordPhrase);
                    foreach (int i in appearances)
                    {
                        SpokenRemarkHighlight srh = new SpokenRemarkHighlight();
                        srh.Id = Guid.NewGuid();
                        srh.SubjectRemark = sr.Id; //Make relationship
                        srh.Category = rk.Category;
                        srh.Rating = rk.RatingWeight;

                        //Find the beginning and ending position
                        loc1 = i;
                        if (loc1 >= 0)
                        {
                            //First, settle it normally
                            srh.BeginPosition = loc1;
                            srh.EndPosition = loc1 + rk.KeywordPhrase.Length - 1;

                            //BUT if this is for a special type of phrase, try to do it differently now.
                            if (rk.KeywordPhrase == "$") //If it is a dollar sign try to find the figure followin the dollar sign
                            {
                                loc2 = sr.Remark.IndexOf(" ", loc1 + 1);
                                if (loc2 > loc1)
                                {
                                    string doltext = sr.Remark.Substring(loc1 + 1, loc2 - loc1 - 1);
                                    float val = float.MaxValue;
                                    try
                                    {
                                        val = Convert.ToSingle(doltext);
                                    }
                                    catch
                                    {
                                        val = float.MaxValue;
                                    }
                                    if (val != float.MaxValue)
                                    {
                                        srh.EndPosition = loc2 - 1;
                                    }
                                }
                            }
                            else if (rk.KeywordPhrase == "%") //If it is a percent sign, get the figure in front if it (percents always trail the number)
                            {
                                loc2 = loc1; //Set the end to the loc1
                                loc1 = sr.Remark.LastIndexOf(" ", loc2) + 1;
                                if (loc2 > loc1)
                                {
                                    string valtxt = sr.Remark.Substring(loc1, loc2 - loc1);
                                    float val = float.MaxValue;
                                    try
                                    {
                                        val = Convert.ToSingle(valtxt);
                                    }
                                    catch
                                    {
                                        val = float.MaxValue;
                                    }
                                    if (val != float.MaxValue)
                                    {
                                        //If successful
                                        srh.BeginPosition = loc1;
                                        srh.EndPosition = loc2;
                                    }
                                }                                
                            }
                        }

                        SpokenRemarkHighlights.Add(srh);
                    }
                }
            }

            #endregion

            //Return to sender!
            ToReturn.CallCompanies = CallCompanies.ToArray();
            ToReturn.EarningsCalls = EarningsCalls.ToArray();
            ToReturn.SpokenRemarks = SpokenRemarks.ToArray();
            ToReturn.CallParticipants = CallParticipants.ToArray();
            ToReturn.SpokenRemarkHighlights = SpokenRemarkHighlights.ToArray();
            return ToReturn;
        }

        public AletheiaEarningsCallProcessingResult ProcessEarningsCall(Transcript trans, string tmf_transcript_url)
        {
            AletheiaEarningsCallProcessingResult res = ProcessEarningsCall(trans);
            res.EarningsCalls[0].Url = tmf_transcript_url;
            return res;
        }


        #region "UTILITY FUNCTIONS"

        public int[] AllIndexOf(string main, string to_find)
        {
            List<int> ToReturn = new List<int>();

            for (int i = main.ToLower().IndexOf(to_find.ToLower()); i > -1; i = main.ToLower().IndexOf(to_find.ToLower(), i + 1))
            {
                ToReturn.Add(i);
            }

            return ToReturn.ToArray();
        }

        private string StripOfUnsafeCharacters(string primary)
        {
            if (primary == null)
            {
                return primary;
            }

            string ToReturn = primary;

            ToReturn = ToReturn.Replace("&amp;", "&");
            ToReturn = ToReturn.Replace("&#39;", "'");
            ToReturn = ToReturn.Replace("&apos", "'");
            ToReturn = ToReturn.Replace("&#x27", "'");

            //Strip quotations or apostraphee's
            ToReturn = ToReturn.Replace("'", "");
            ToReturn = ToReturn.Replace("\"", "");

            return ToReturn;
        }

        #endregion

    }
}