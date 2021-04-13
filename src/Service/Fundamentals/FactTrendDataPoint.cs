using System;
using Aletheia.Service;

namespace Aletheia.Service.Fundamentals
{
    public class FactTrendDataPoint
    {
        public DateTime PeriodStart {get; set;}
        public DateTime PeriodEnd {get; set;}
        public float Value {get; set;}
    }
}