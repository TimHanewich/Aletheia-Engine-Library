using System;

namespace Aletheia.Engine.ProcessingQueue
{
    public class SecFilingTaskDetails
    {
        public Guid Id {get; set;}
        public Guid ParentTask {get; set;}
        public string FilingUrl {get; set;}
    }
}