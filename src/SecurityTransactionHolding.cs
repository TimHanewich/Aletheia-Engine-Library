using System;

namespace Aletheia
{
    public class SecurityTransactionHolding
    {
        //Standard data - always there regardless of whether or not it is a transaction or holding!
        public Guid Id {get; set;}
        public Guid FromFiling {get; set;}
        public SecFiling _FromFiling {get; set;}
        public TransactionHoldingEntryType EntryType {get; set;}
        public float QuantityOwnedFollowingTransaction {get; set;}
        public DirectIndirect DirectIndirect {get; set;}
        public string SecurityTitle {get; set;}
        public SecurityType SecurityType {get; set;}

        //Transaction-Related!
        public AcquiredDisposed? AcquiredDisposed {get; set;}
        public float? Quantity {get; set;}
        public float? PricePerSecurity {get; set;}
        public DateTime? TransactionDate {get; set;}
        public SecuritiesExchangeCommission.Edgar.TransactionType? TransactionCode {get; set;}
        
        //Derivative-specific data
        public float? ConversionOrExcercisePrice {get; set;}
        public DateTime? ExcercisableDate {get; set;}
        public DateTime? ExpirationDate {get; set;}
        public string UnderlyingSecurityTitle {get; set;}
        public float? UnderlyingSecurityQuantity {get; set;}
    }
}