using System;
using Aletheia.Service;
using Aletheia.InsiderTrading;
using SecuritiesExchangeCommission.Edgar;

namespace Aletheia.Engine.Cloud.Webhooks
{
    public class InsiderTradingWebhookSubscription
    {
        public Guid Id {get; set;}
        public Guid Subscription {get; set;}
        public long? IssuerCik {get; set;}
        public long? OwnerCik {get; set;}
        public SecurityType? SecurityType {get; set;}
        public TransactionType? TransactionType {get; set;}
    }
}