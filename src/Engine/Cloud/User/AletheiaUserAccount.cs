using System;

namespace Aletheia.Engine.Cloud.User
{
    public class AletheiaUserAccount
    {
        private static string username_allowed_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";
        private static string password_disallowed_chars = "= ";

        public Guid Id {get; set;}
        public string Username {get; set;}
        public string Password {get; set;}
        public string Email {get; set;}
        public DateTime CreatedAtUtc {get; set;}

        public static bool UsernameValid(string username_)
        {
            foreach (char c in username_)
            {
                if (username_allowed_chars.Contains(c.ToString()) == false)
                {
                    return false;
                }
            }
            return true;
        }

        public static bool PasswordValid(string password_)
        {
            foreach (char c in password_)
            {
                if (password_disallowed_chars.Contains(c.ToString()))
                {
                    return false;
                }
            }
            return true;
        }

    }
}