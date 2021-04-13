using System;
using Aletheia.Service;

namespace Aletheia.Service.Fundamentals
{
    public class FinancialFactTrendDataPoint
    {
        public DateTime PeriodStart {get; set;}
        public DateTime PeriodEnd {get; set;}
        public float Value {get; set;}
    }
}