using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;

namespace Aletheia
{
    public class AletheiaProcessor
    {
        
        public AletheiaProcessingResult ProcessForm4(string xml, string sec_accession_num, string filing_url)
        {
            //Create the form4
            StatementOfChangesInBeneficialOwnership form4 = StatementOfChangesInBeneficialOwnership.ParseXml(xml);
            return ProcessForm4(form4, sec_accession_num, filing_url);
        }
    
        public AletheiaProcessingResult ProcessForm4(StatementOfChangesInBeneficialOwnership form4, string sec_accession_num, string filing_url)
        {
            AletheiaProcessingResult ToReturn = new AletheiaProcessingResult();
            List<SecEntity> ToAppend_SecEntities = new List<SecEntity>();
            List<SecFiling> ToAppend_SecFilings = new List<SecFiling>();
            List<HeldOfficerPosition> ToAppend_HeldOfficerPositions = new List<HeldOfficerPosition>();
            List<SecurityTransactionHolding> ToAppend_SecurityTransactionHoldings = new List<SecurityTransactionHolding>();

            //Get issuer entity
            SecEntity issuer = new SecEntity();
            issuer.Cik = Convert.ToInt64(form4.IssuerCik);
            issuer.Name = form4.IssuerName.Trim();
            issuer.TradingSymbol = form4.IssuerTradingSymbol.Trim().ToUpper();
            ToAppend_SecEntities.Add(issuer);

            //Get owner entity
            SecEntity owner = new SecEntity();
            owner.Cik = Convert.ToInt64(form4.OwnerCik);
            owner.Name = form4.OwnerName.Trim();
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
            filing.ReportedOn = form4.PeriodOfReport;
            filing.Issuer = issuer.Cik;
            filing.Owner = owner.Cik;
            ToAppend_SecFilings.Add(filing);


            //Now do each transaction holding - non-derivative first
            foreach (NonDerivativeTransaction ndt in form4.NonDerivativeTransactions)
            {
                SecurityTransactionHolding sth = new SecurityTransactionHolding();
                sth.Id = Guid.NewGuid();
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
                        sth.AcquiredDisposed = AcquiredDisposed.Acquired;
                    }
                    else if (ndt.AcquiredOrDisposed ==  SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Disposed)
                    {
                        sth.AcquiredDisposed = AcquiredDisposed.Disposed;
                    }

                    //Quantity (transaction related)
                    sth.Quantity = ndt.TransactionQuantity.Value;

                    //Price per security
                    sth.PricePerSecurity = ndt.TransactionPricePerSecurity.Value;

                    //Transaction Date
                    sth.TransactionDate = ndt.TransactionDate.Value;

                    //Transaction Code
                    sth.TransactionCode = ndt.TransactionCode.Value;
                }

                //Quantity owned following transaction
                sth.QuantityOwnedFollowingTransaction = ndt.SecuritiesOwnedFollowingTransaction;

                //Direct or indirect ownership?
                if (ndt.DirectOrIndirectOwnership == OwnershipNature.Direct)
                {
                    sth.OwnershipType = DirectIndirect.Direct;
                }
                else if (ndt.DirectOrIndirectOwnership == OwnershipNature.Indirect)
                {
                    sth.OwnershipType = DirectIndirect.Indirect;
                }

                //Security Title
                sth.SecurityTitle = ndt.SecurityTitle;

                //Security type
                sth.SecurityType = SecurityType.NonDerivative;

                ToAppend_SecurityTransactionHoldings.Add(sth);                
            }

            //Now do each transaction holding - derivative
            foreach (DerivativeTransaction dt in form4.DerivativeTransactions)
            {
                SecurityTransactionHolding sth = new SecurityTransactionHolding();
                sth.Id = Guid.NewGuid();
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
                        sth.AcquiredDisposed = AcquiredDisposed.Acquired;
                    }
                    else if (dt.AcquiredOrDisposed ==  SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Disposed)
                    {
                        sth.AcquiredDisposed = AcquiredDisposed.Disposed;
                    }

                    //Quantity (transaction related)
                    sth.Quantity = dt.TransactionQuantity.Value;

                    //Price per security
                    sth.PricePerSecurity = dt.TransactionPricePerSecurity.Value;

                    //Transaction Date
                    sth.TransactionDate = dt.TransactionDate.Value;

                    //Transaction Code
                    sth.TransactionCode = dt.TransactionCode.Value;
                }

                //Quantity owned following transaction
                sth.QuantityOwnedFollowingTransaction = dt.SecuritiesOwnedFollowingTransaction;

                //Direct or indirect ownership?
                if (dt.DirectOrIndirectOwnership == OwnershipNature.Direct)
                {
                    sth.OwnershipType = DirectIndirect.Direct;
                }
                else if (dt.DirectOrIndirectOwnership == OwnershipNature.Indirect)
                {
                    sth.OwnershipType = DirectIndirect.Indirect;
                }

                //Security Title
                sth.SecurityTitle = dt.SecurityTitle;

                //Security type
                sth.SecurityType = SecurityType.Derivative;

                //If the security type is non-derivative (it should be in this case every time because that is the type of stuff we are looping through right now), apply those properties

                //Conversion or excercise price
                if (dt.ConversionOrExcercisePrice.HasValue)
                {
                    sth.ConversionOrExcercisePrice = dt.ConversionOrExcercisePrice.Value;
                }
                
                //Excercisable date
                if (dt.Excersisable.HasValue)
                {
                    sth.ExcercisableDate = dt.Excersisable.Value;
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

            //Append and return
            ToReturn.SecEntities = ToAppend_SecEntities.ToArray();
            ToReturn.SecFilings = ToAppend_SecFilings.ToArray();
            ToReturn.HeldOfficerPositions = ToAppend_HeldOfficerPositions.ToArray();
            ToReturn.SecurityTransactionHoldings = ToAppend_SecurityTransactionHoldings.ToArray();
            return ToReturn;
        }

    }
}