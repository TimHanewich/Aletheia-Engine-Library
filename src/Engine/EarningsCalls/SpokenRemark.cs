using System;

namespace Aletheia.Engine.EarningsCalls
{
    public class SpokenRemark
    {
        public Guid Id {get; set;}
        public Guid FromCall {get; set;}
        public Guid SpokenBy {get; set;}
        public int SequenceNumber {get; set;}

        //Not actually part of the SQL table, but here for convenience in C#
        public string Remark {get; set;}
    }
}