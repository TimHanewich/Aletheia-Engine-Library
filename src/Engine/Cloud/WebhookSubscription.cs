using System;

namespace Aletheia.Engine.Cloud
{
    public class WebhookSubscription
    {
        public string Endpoint {get; set;}
        public DateTime AddedAtUtc {get; set;}
        public Guid RegisteredToKey {get; set;}
    }
}