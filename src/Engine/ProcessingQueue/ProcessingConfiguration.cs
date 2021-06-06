using System;

namespace Aletheia.Engine.ProcessingQueue
{
    public class ProcessingConfiguration
    {
        public byte Id {get; set;}
        public bool InternalProcessingPaused {get; set;}
        public int ResumeInternalProcessingInSeconds {get; set;}
    }
}