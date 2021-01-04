using System;

namespace Aletheia
{
    public class Security
    {
        public string CompanyCik {get; set;} //The CIK of the company this security is for
        public string Title {get; set;}
        public SecurityType SecurityType {get; set;}
    }
}