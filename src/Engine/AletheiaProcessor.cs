using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Aletheia.InsiderTrading;
using System.IO;
using Aletheia.Engine.EarningsCalls;
using TheMotleyFool.Transcripts;

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
            
            //Extract the trading symbol and company name from the title
            loc2 = trans.Title.LastIndexOf(")");
            loc1 = trans.Title.LastIndexOf("(", loc2);
            if (loc2 > loc1)
            {
                cc.TradingSymbol = trans.Title.Substring(loc1 + 1, loc2 - loc1 - 1).Trim().ToUpper();
                cc.Name = trans.Title.Substring(0, loc1 - 1).Trim();
            }
            else
            {
                cc.Name = null;
                cc.TradingSymbol = null;
            }

            CallCompanies.Add(cc);

            #endregion

            #region "Get the EarningsCall"

            EarningsCall ec = new EarningsCall();
            ec.Id = Guid.NewGuid();
            ec.ForCompany = cc.Id;
            ec.Url = null;
            ec.Title = trans.Title;

            //Get the Quarter (fiscal period)
            loc1 = trans.Title.LastIndexOf(")");
            loc1 = trans.Title.IndexOf(" ", loc1);
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

            //Get the year
            loc1 = trans.Title.LastIndexOf(")");
            loc1 = trans.Title.IndexOf(" ", loc1);
            loc1 = trans.Title.IndexOf(" ", loc1 + 1);
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

            //Held At
            ec.HeldAt = trans.CallDateTimeStamp;

            //Add it!
            EarningsCalls.Add(ec);

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

    }
}