using System;

namespace Aletheia.Fundamentals
{
    public class FinancialFact
    {
        public Guid Id {get; set;}
        public Guid ParentContext {get; set;}
        public FactContext _ParentContext {get; set;}
        public FactLabel LabelId {get; set;}
        public float Value {get; set;}
    }
}