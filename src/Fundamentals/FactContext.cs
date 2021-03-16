using System;

namespace Aletheia.Fundamentals
{
    public class FactContext
    {
        public Guid Id {get; set;}
        public Guid? FromFiling {get; set;}
        public SecFiling _FromFiling {get; set;}
        public DateTime PeriodStart {get; set;}
        public DateTime PeriodEnd {get; set;}
    }
}