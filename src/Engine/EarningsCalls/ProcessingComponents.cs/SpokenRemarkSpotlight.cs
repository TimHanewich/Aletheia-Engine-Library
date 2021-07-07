using System;
using Aletheia.Engine.EarningsCalls;

namespace Aletheia.Engine.EarningsCalls.ProcessingComponents
{
    //This class is used for getting spoken remarks of interest.
    public class SpokenRemarkSpotlight
    {
        public string Remark {get; set;}
        public CallParticipant SpokenBy {get; set;}
        public SpokenRemarkHighlight[] Highlights {get; set;}
    }
}