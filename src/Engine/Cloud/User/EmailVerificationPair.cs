using System;

namespace Aletheia.Engine.Cloud.User
{
    public class EmailVerificationCodePair
    {
        public string Email {get; set;}
        public string Code {get; set;}
        public DateTime StartedAtUtc {get; set;}

        public static EmailVerificationCodePair Create(string email)
        {
            EmailVerificationCodePair ToReturn = new EmailVerificationCodePair();

            ToReturn.Email = email;
            ToReturn.Code = Guid.NewGuid().ToString().Replace("-", "").Substring(0, 5).ToUpper();
            ToReturn.StartedAtUtc = DateTime.UtcNow;

            return ToReturn;
        }

    }
}