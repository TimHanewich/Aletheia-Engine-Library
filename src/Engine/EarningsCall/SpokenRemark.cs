using System;

namespace Aletheia.Engine.EarningsCall
{
    public class SpokenRemark
    {
        public Guid Id {get; set;}
        public Guid FromCall {get; set;}
        public Guid SpokenBy {get; set;}
        public Guid BlobId {get; set;}
    }
}