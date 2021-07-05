using System;

namespace Aletheia.Engine.EarningsCalls.ProcessingComponents
{
    public class RemarkKeyword
    {
        public string KeywordPhrase {get; set;}
        public HighlightCategory Category {get; set;}
        public float RatingWeight {get; set;}

        public RemarkKeyword()
        {

        }

        public RemarkKeyword(string phrase, HighlightCategory category, float weight)
        {
            KeywordPhrase = phrase;
            Category = category;
            RatingWeight = weight;
        }
    }
}