using System;
using SecuritiesExchangeCommission.Edgar;

namespace Aletheia
{
    public class SecurityTransaction
    {
        public string SecAccessionNumber {get; set;}
        public AcquiredDisposed AcquiredDisposed {get; set;}
        public float Quantity {get; set;}
        public DateTime TransactionDate {get; set;}
        public byte TransactionCode {get; set;}
        public float QuantityOwnedFollowingTransaction {get; set;}
        public OwnershipNature DirectIndirect {get; set;}
    }
}