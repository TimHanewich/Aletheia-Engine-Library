using System;

namespace Aletheia.Fundamentals
{
    public class FactContext
    {
        public Guid Id {get; set;}
        public Guid? FromFiling {get; set;}
        public SecFiling _FromFiling {get; set;}
        public DateTime Start {get; set;}
        public DateTime End {get; set;}
    }
}