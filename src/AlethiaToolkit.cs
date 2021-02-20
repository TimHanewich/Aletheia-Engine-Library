using System;
using System.Collections.Generic;
using SecuritiesExchangeCommission.Edgar;
using System.Threading.Tasks;

namespace Aletheia
{
    public class AletheiaToolkit
    {
        
        #region "String manipulation"

        public static string NormalizeAndRearrangeForm4Name(string original)
        {
            if (original.ToLower().Contains("inc") || original.ToLower().Contains(",")) //For example, if it is a "Intel Americas, Inc."
            {
                return original;
            }
            else
            {
                string[] parts = original.Split(' ');
                if (parts.Length == 2) //For example "Wilson Andrew", it should be "Andrew Wilson"
                {
                    return ToNormalcase(parts[1]) + " " + ToNormalcase(parts[0]);
                }
                else if (parts.Length == 3) //They probably included thier middle initial like "PETERSON SANDRA E"
                {
                    return ToNormalcase(parts[1]) + " " + ToNormalcase(parts[2]) + " " + ToNormalcase(parts[0]);
                }
                else if (parts.Length == 4)
                {
                    //Examples of this:
                    //FORD WILLIAM CLAY JR
                    //EARLEY ANTHONY F JR
                    //Huntsman Jon M Jr
                    //FARLEY JR JAMES D
                    //FORD EDSEL B II
                    //FARLEY JR JAMES D

                    //Get a list of all suffixes
                    List<string> Suffixes = new List<string>();
                    Suffixes.Add("jr");
                    Suffixes.Add("sr");
                    Suffixes.Add("ii");
                    Suffixes.Add("iii");
                    Suffixes.Add("iv");

                    //Find the suffix being used
                    string suffix = "";
                    foreach (string s in parts)
                    {
                        if (Suffixes.Contains(s.ToLower()))
                        {
                            suffix = s;
                        }
                    }

                    //If there are 4 names and not a suffix, just return the same thing normalized. Give up!
                    if (suffix == "")
                    {
                        string ToReturn = "";
                        foreach (string s in parts)
                        {
                            ToReturn = ToReturn + ToNormalcase(s) + " ";
                        }
                        ToReturn = ToReturn.Substring(0, ToReturn.Length - 1); 
                        return ToReturn;
                    }

                    //What index is the suffix?
                    int suffix_index = 0;
                    for (int i = 0; i < parts.Length; i++)
                    {
                        if (parts[i].ToLower() == suffix.ToLower())
                        {
                            suffix_index = i;
                        }
                    }

                    //Now that we have the index of where the suffix lands, now we should know how to rearrange the name
                    if (suffix_index == 3) //Last (most common I think)
                    {
                        //1, 2, 0, 3 is the order
                        string ToReturn = ToNormalcase(parts[1]) + " " + ToNormalcase(parts[2]) + " " + ToNormalcase(parts[0]) + " " + parts[3].ToUpper();
                        return ToReturn;
                    }
                    else if (suffix_index == 1)
                    {
                        //Order: 2, 3, 0, 1
                        string ToReturn = ToNormalcase(parts[2]) + " " + ToNormalcase(parts[3]) + " " + ToNormalcase(parts[0]) + " " + parts[1].ToUpper();
                        return ToReturn;
                    }
                    else
                    {
                        //Just return the same thing with normal case
                        string ToReturn = "";
                        foreach (string s in parts)
                        {
                            ToReturn = ToReturn + ToNormalcase(s) + " ";
                        }
                        ToReturn = ToReturn.Substring(0, ToReturn.Length - 1);
                        return ToReturn;
                    }
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
    
        public static string NormalizeAllWords(string original)
        {
            string[] parts = original.Split(' ');
            string ToReturn = "";
            foreach (string s in parts)
            {
                ToReturn = ToReturn + ToNormalcase(s) + " ";
            }
            ToReturn = ToReturn.Substring(0, ToReturn.Length - 1); //Remove the last space
            return ToReturn;
        }

        #endregion
        
        public static async Task<EdgarSearchResult[]> CollectAllFilingsOfTypeAsync(string symbol_or_cik, string filing_type, bool allow_amendments = true)
        {
            //Get all filings
            List<EdgarSearchResult> RESULTS = new List<EdgarSearchResult>();
            bool Kill = false;
            EdgarSearch es = await EdgarSearch.CreateAsync(symbol_or_cik, filing_type, null, EdgarSearchOwnershipFilter.only);
            while (Kill == false)
            {
                foreach (EdgarSearchResult esr in es.Results)
                {

                    //Should we add this one?
                    bool AddFiling = false;
                    if (esr.Filing.Trim().ToLower() == filing_type.Trim().ToLower())
                    {
                        AddFiling = true;
                    }
                    else
                    {
                        if (allow_amendments)
                        {
                            if (esr.Filing.Trim().ToLower() == filing_type.Trim().ToLower() + "/a")
                            {
                                AddFiling = true;
                            }
                        }
                    }

                    //If we should add it, then add it!
                    if (AddFiling)
                    {
                        RESULTS.Add(esr);
                    }
                }
                
                //Paging
                if (es.NextPageAvailable())
                {
                    es = es.NextPageAsync().Result;
                }
                else
                {
                    Kill = true;
                }
            }

            return RESULTS.ToArray();
        }

        
    }
}