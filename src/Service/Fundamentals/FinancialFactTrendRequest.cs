using System;
using Aletheia.Fundamentals;

namespace Aletheia.Service.Fundamentals
{
    public class FinancialFactTrendRequest
    {
        public string Id {get; set;}
        public FactLabel Label {get; set;}
        public PeriodType? PeriodType {get; set;}
        public DateTime? After {get; set;}
        public DateTime? Before {get; set;}
    }
}