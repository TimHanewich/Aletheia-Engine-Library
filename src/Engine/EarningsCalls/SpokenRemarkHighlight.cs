using System;

namespace Aletheia.Engine.EarningsCalls
{
    public class SpokenRemarkHighlight
    {
        public Guid Id {get; set;}
        public Guid SubjectRemark {get; set;}
        public int BeginPosition {get; set;}
        public int EndPosition {get; set;}
        public HighlightCategory Category {get; set;}
    }
}