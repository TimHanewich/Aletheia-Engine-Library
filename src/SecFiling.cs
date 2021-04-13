using System;
using Aletheia.InsiderTrading;

namespace Aletheia.Engine
{
    public class SecFiling
    {
        public Guid Id {get; set;}
        public string FilingUrl {get; set;}
        public long AccessionP1 {get; set;}
        public int AccessionP2 {get; set;}
        public int AccessionP3 {get; set;}
        public FilingType FilingType {get; set;}
        public DateTime ReportedOn {get; set;}
        public long Issuer {get; set;}
        public SecEntity _Issuer {get; set;}
        public long? Owner {get; set;}
        public SecEntity _Owner {get; set;}
    }
}