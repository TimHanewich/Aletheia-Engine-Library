using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Aletheia.InsiderTrading;

namespace Aletheia.Engine
{
    public class AletheiaProcessor
    {

        public async Task<AletheiaProcessingResult> ProcessStatementOfBeneficialOwnershipAsync(string filing_url)
        {
            EdgarSearchResult esr = new EdgarSearchResult();
            esr.DocumentsUrl = filing_url;   
            FilingDocument[] docs = await esr.GetDocumentFormatFilesAsync();
            foreach (FilingDocument fd in docs)
            {
                if (fd.DocumentName.Trim().ToLower().Contains(".xml"))
                {
                    HttpClient hc = new HttpClient();
                    HttpRequestMessage req = SecToolkit.PrepareHttpRequestMessage();
                    req.RequestUri = new Uri(fd.Url);
                    HttpResponseMessage hrm = await hc.SendAsync(req);
                    string content = await hrm.Content.ReadAsStringAsync();
                    string accession_num = await esr.GetAccessionNumberAsync();
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

    }
}