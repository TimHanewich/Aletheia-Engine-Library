using System;

namespace Aletheia.Engine.EarningsCalls
{
    public class AletheiaEarningsCallProcessingResult
    {
        public CallCompany[] CallCompanies {get; set;}
        public EarningsCall[] EarningsCalls {get; set;}
        public SpokenRemark[] SpokenRemarks {get; set;}
        public CallParticipant[] CallParticipants {get; set;}
        public SpokenRemarkHighlight[] SpokenRemarkHighlights {get; set;}

        public SpokenRemark FindSpokenRemark(Guid id)
        {
            foreach (SpokenRemark sr in SpokenRemarks)
            {
                if (sr.Id == id)
                {
                    return sr;
                }
            }
            throw new Exception("Unable to find SpokenRemark with Id '" + id.ToString() + "'");
        }

        public CallParticipant FindCallParticipant(Guid id)
        {
            foreach (CallParticipant cp in CallParticipants)
            {
                if (cp.Id == id)
                {
                    return cp;
                }
            }
            throw new Exception("Unable to find CallParticipant with Id '" + id.ToString() + "'");
        }
    }
}