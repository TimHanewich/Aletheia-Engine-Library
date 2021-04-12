using System;

namespace Aletheia.Engine.Fundamentals
{
    public enum FactLabel
    {
        //Income Statement Items
            Revenue = 0,
            SellingGeneralAndAdministrativeExpense = 1,
            ResearchAndDevelopmentExpense = 2,
            OperatingIncome = 3,
            NetIncome = 4,

            //Balance Sheet Items
            Assets = 5,
            Liabilities = 6,
            Equity = 7,
            Cash = 8,
            CurrentAssets = 9,
            CurrentLiabilities = 10,
            RetainedEarnings = 11,
            CommonStockSharesOutstanding = 12,
            

            //Cash Flow Statement Items
            OperatingCashFlows = 13,
            InvestingCashFlows = 14,
            FinancingCashFlows = 15,
            ProceedsFromIssuanceOfDebt = 16,
            PaymentsOfDebt = 17,
            DividendsPaid = 18,
    }
}