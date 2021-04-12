using System;

namespace Aletheia.Engine
{
    public class AletheiaProcessingResult
    {
        public SecEntity[] SecEntities {get; set;}
        public SecFiling[] SecFilings {get; set;}
        public HeldOfficerPosition[] HeldOfficerPositions {get; set;}
        public SecurityTransactionHolding[] SecurityTransactionHoldings {get; set;}
    }
}