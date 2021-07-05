using System;

namespace Aletheia.Engine.EarningsCalls
{
    public class AletheiaEarningsCallsProcessingResult
    {
        public CallCompany[] CallCompanies {get; set;}
        public EarningsCall[] EarningsCalls {get; set;}
        public SpokenRemark[] SpokenRemarks {get; set;}
        public CallParticipant[] CallParticipants {get; set;}
        public SpokenRemarkHighlight[] SpokenRemarkHighlights {get; set;}
    }
}