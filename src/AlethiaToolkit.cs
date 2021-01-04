using System;

namespace Aletheia
{
    public class AletheiaToolkit
    {
        public static string NormalizeAndRearrangeForm4Name(string original)
        {
            if (original.ToLower().Contains("inc") || original.ToLower().Contains(",")) //For example, if it is a "Intel Americas, Inc."
            {
                return original;
            }
            else
            {
                string[] parts = original.Split(" ");
                if (parts.Length == 2) //For example "Wilson Andrew", it should be "Andrew Wilson"
                {
                    return ToNormalcase(parts[1]) + " " + ToNormalcase(parts[0]);
                }
                else if (parts.Length == 3) //They probably included thier middle initial like "PETERSON SANDRA E"
                {
                    return ToNormalcase(parts[1]) + " " + ToNormalcase(parts[2]) + " " + ToNormalcase(parts[0]);
                }
                else
                {
                    return original;
                }
            }
        }

        public static string ToNormalcase(string original)
        {
            if (original == null || original == "")
            {
                return original;
            }
            else
            {
                string Part1 = original.Substring(0, 1).ToUpper();
                string Part2 = original.Substring(1).ToLower();
                return Part1 + Part2;
            }
        }
    }
}