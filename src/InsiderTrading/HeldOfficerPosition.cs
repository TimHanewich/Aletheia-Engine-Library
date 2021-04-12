using System;

namespace Aletheia.Engine
{
    public class HeldOfficerPosition
    {
        public Guid Id {get; set;}
        public long Officer {get; set;}
        public long Company {get; set;}
        public string PositionTitle {get; set;}
        public Guid ObservedOn {get; set;} //The filing that this officer title was observed on. (Guid relates to the record in SQL)
    }
}