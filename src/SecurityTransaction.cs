using System;
using SecuritiesExchangeCommission.Edgar;

namespace Aletheia
{
    public class SecurityTransaction
    {
        public Person OwnedBy {get; set;} //References the person object that owns this.
        public Security SubjectSecurity {get; set;} //References the security that was transacted on.
        public string SecAccessionNumber {get; set;}
        public AcquiredDisposed? AcquiredDisposed {get; set;}
        public float? Quantity {get; set;}
        public DateTime? TransactionDate {get; set;}
        public TransactionType? TransactionCode {get; set;}
        public float QuantityOwnedFollowingTransaction {get; set;}
        public OwnershipNature DirectIndirect {get; set;}
        public DateTime ReportedOn {get; set;}
    }
}