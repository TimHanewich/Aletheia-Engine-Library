using System;

namespace Aletheia.Engine.ProcessingQueue
{
    public class ProcessingTask
    {
        public Guid Id {get; set;}
        public DateTime AddedAtUtc {get; set;}
        public TaskType TaskType {get; set;}
        public int PriorityLevel {get; set;}
    }
}