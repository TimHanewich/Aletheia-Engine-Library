using System;

namespace Aletheia.Engine.EarningsCallTranscripts
{
    public class EarningsCallTranscript
    {
        public Guid Id {get; set;}
        public Guid ForCompany {get; set;}
        public string Url {get; set;}
        public string Title {get; set;}
        public FiscalPeriod Period {get; set;}
        public int Year {get; set;}
        public DateTime HeldAt {get; set;}
    }
}