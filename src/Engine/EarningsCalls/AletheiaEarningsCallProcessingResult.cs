using System;
using System.Collections.Generic;

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
    
        public void PrintSpokenRemarkHighlight(SpokenRemarkHighlight srh)
        {
            SpokenRemark sr = FindSpokenRemark(srh.SubjectRemark);
            CallParticipant cp = FindCallParticipant(sr.SpokenBy);

            //Clip the highlight out
            string[] parts = ClipHighlight(sr.Remark, srh.BeginPosition, srh.EndPosition);

            //Print it
            Console.WriteLine(cp.Name + " (" + cp.Title + "): ");
            Console.Write(parts[0]);
            ConsoleColor oc = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.Write(parts[1]);
            Console.ForegroundColor = oc;
            Console.WriteLine(parts[2]);
        }

        //This will take the entire remark and return an array of 3 strings: the beginning of the sentence, the highlight itself, and the end of the sentence.
        private string[] ClipHighlight(string remark, int start, int end)
        {
            List<string> Parts = new List<string>();

            string p1 = remark.Substring(0, start);
            string p2 = remark.Substring(start, end - start);
            string p3 = remark.Substring(end);
            Parts.Add(p1);
            Parts.Add(p2);
            Parts.Add(p3);

            return Parts.ToArray();
        }
    }
}