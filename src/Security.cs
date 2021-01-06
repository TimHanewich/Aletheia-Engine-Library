using System;

namespace Aletheia
{
    public class Security
    {
        public Company Company {get; set;}
        public string Title {get; set;}
        public SecurityType SecurityType {get; set;}

        //Derivative base (null if not derivative or not available)
        public float? ConversionOrExcercisePrice {get; set;}
        public DateTime? ExcercisableDate {get; set;}
        public DateTime? ExpirationDate {get; set;}
        public string UnderlyingSecurityTitle {get; set;}
        public float UnderlyingSecurityQuantity {get; set;}
    }
}