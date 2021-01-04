using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;

namespace Aletheia
{
    public class AletheiaProcessor
    {
        public SecurityTransaction[] ProcessForm4(string xml, string sec_accession_num)
        {
            //Create the form4
            StatementOfChangesInBeneficialOwnership form4 = StatementOfChangesInBeneficialOwnership.ParseXml(xml);

            //Create the Person
            Person ThePerson = new Person();
            ThePerson.CIK = form4.OwnerCik;
            ThePerson.FullName = AletheiaToolkit.NormalizeAndRearrangeForm4Name(form4.OwnerName);

            //Create the company
            Company TheCompany = new Company();
            TheCompany.CIK = form4.IssuerCik;
            TheCompany.TradingSymbol = form4.IssuerTradingSymbol;
            TheCompany.Name = form4.IssuerName;

            #region "Create a comprehensive list of unique securities that were used in this form 4"

            List<Security> UniqueSecuritiesInThisForm4 = new List<Security>();

            //First start with non derivative holdings
            foreach (NonDerivativeHolding holding in form4.NonDerivativeHoldings)
            {
                //Check if we already have this security in the unique list
                bool AlreadyHaveThisSecurity = SecurityExists(UniqueSecuritiesInThisForm4, holding.SecurityTitle);
                if (AlreadyHaveThisSecurity == false)
                {
                    Security s = new Security();
                    s.Company = TheCompany;
                    s.SecurityType = SecurityType.NonDerivative;
                    s.Title = holding.SecurityTitle;
                    UniqueSecuritiesInThisForm4.Add(s);
                }
            }

            //Next do the non derivative transactions
            foreach (NonDerivativeTransaction transaction in form4.NonDerivativeTransactions)
            {
                bool AlreadyHaveThisSecurity = SecurityExists(UniqueSecuritiesInThisForm4, transaction.SecurityTitle);
                if (AlreadyHaveThisSecurity == false)
                {
                    Security s = new Security();
                    s.Company = TheCompany;
                    s.SecurityType = SecurityType.NonDerivative;
                    s.Title = transaction.SecurityTitle;
                    UniqueSecuritiesInThisForm4.Add(s);
                }
            }

            //Finally, do it for the derivative transactions
            foreach (DerivativeTransaction transaction in form4.DerivativeTransactions)
            {
                bool AlreadyHaveThisSecurity = SecurityExists(UniqueSecuritiesInThisForm4, transaction.DerivativeSecurityTitle);
                if (AlreadyHaveThisSecurity == false)
                {
                    Security s = new Security();
                    s.Company = TheCompany;
                    s.SecurityType = SecurityType.Derivative;
                    s.Title = transaction.DerivativeSecurityTitle;
                    UniqueSecuritiesInThisForm4.Add(s);
                }
            }


            #endregion

            #region "Go through each transaction and create a security transaction"

            List<SecurityTransaction> SecurityTransactions = new List<SecurityTransaction>();

            //First start with non derivative holdings
            foreach (NonDerivativeHolding holding in form4.NonDerivativeHoldings)
            {
                SecurityTransaction st = new SecurityTransaction();
                st.OwnedBy = ThePerson;
                st.SubjectSecurity = SelectSecurityFromListByName(UniqueSecuritiesInThisForm4, holding.SecurityTitle);
                st.SecAccessionNumber = sec_accession_num;
                st.AcquiredDisposed = null; //Null because this is a holding, not a transaction
                st.Quantity = null; //Null because this is a holding, not a transaction
                st.TransactionDate = null; //Null because this is a holding, not a transaction
                st.TransactionCode = null; //Null because this is a holding, not a transaction
                st.QuantityOwnedFollowingTransaction = holding.SharesOwnedFollowingTransaction;
                st.DirectIndirect = holding.DirectOrIndirectOwnership;
                SecurityTransactions.Add(st);
            }

            //next do non derivative transactions
            foreach (NonDerivativeTransaction transaction in form4.NonDerivativeTransactions)
            {
                SecurityTransaction st = new SecurityTransaction();
                st.OwnedBy = ThePerson;
                st.SubjectSecurity = SelectSecurityFromListByName(UniqueSecuritiesInThisForm4, transaction.SecurityTitle);
                st.SecAccessionNumber = sec_accession_num;
                st.AcquiredDisposed = transaction.AcquiredOrDisposed;
                st.Quantity = transaction.TransactionShares;
                st.TransactionDate = transaction.TransactionDate;
                st.TransactionCode = transaction.TransactionCode;
                st.QuantityOwnedFollowingTransaction = transaction.SharesOwnedFollowingTransaction;
                st.DirectIndirect = transaction.DirectOrIndirectOwnership;
                SecurityTransactions.Add(st);
            }

            //Finally, for derivative transactions
            foreach (DerivativeTransaction transaction in form4.DerivativeTransactions)
            {
                SecurityTransaction st = new SecurityTransaction();
                st.OwnedBy = ThePerson;
                st.SubjectSecurity = SelectSecurityFromListByName(UniqueSecuritiesInThisForm4, transaction.DerivativeSecurityTitle);
                st.SecAccessionNumber = sec_accession_num;
                st.AcquiredDisposed = transaction.AcquiredOrDisposed;
                st.Quantity = transaction.Quantity;
                st.TransactionDate = transaction.TransactionDate;
                st.TransactionCode = transaction.TransactionCode;
                st.QuantityOwnedFollowingTransaction = transaction.Quantity;
                st.DirectIndirect = transaction.DirectOrIndirectOwnership;
                SecurityTransactions.Add(st);
            }

            #endregion
            
            return SecurityTransactions.ToArray();
        }
    
        #region "Utility Functions"

        private bool SecurityExists(List<Security> list, string security_title)
        {
            bool ToReturn = false;
            foreach (Security s in list)
            {
                if (s.Title.ToLower() == security_title.ToLower())
                {
                    ToReturn = true;
                }
            }
            return ToReturn;
        }

        private Security SelectSecurityFromListByName(List<Security> list, string security_title)
        {
            foreach (Security s in list)
            {
                if (s.Title.ToLower() == security_title.ToLower())
                {
                    return s;
                }
            }
            throw new Exception("Unable to find security with title '" + security_title + "' in list.");
        }

        #endregion
    }
}