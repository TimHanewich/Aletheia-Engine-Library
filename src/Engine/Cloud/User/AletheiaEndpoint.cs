using System;

namespace Aletheia.Engine.Cloud.User
{
    public enum AletheiaEndpoint
    {
        AffiliatedOwners = 0,
        Consumption = 1,
        EntityFilings = 2,
        FinancialFactTrend = 3,
        Financials = 4,
        LatestTransactions = 5,
        MyCalls = 6,
        NewFilings = 7,
        OpenCommonFinancials = 8,
        OpenForm4 = 9,
        SearchEntities = 10,
        StockData = 11,
        SubscribeToNewFilingsWebhook = 12,
        UnsubscribeFromNewFilingsWebhookById = 13,
        GetEntity = 14,
        GetFiling = 15,
        UnsubscribeFromNewFilingsWebhookByEndpoint = 16,
        InsiderTradingHook = 17, //This is when a the Aletheia API will trigger someone's API endpoint via a webhook trigger (push direction)
        SubscribeToInsiderTradingWebhook = 18,
        UnsubscribeWebhook = 19
    }
}