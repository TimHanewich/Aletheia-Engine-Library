using System;
using Aletheia;
using Aletheia.InsiderTrading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Newtonsoft.Json;
using SecuritiesExchangeCommission.Edgar;

namespace Aletheia.Service.InsiderTrading
{
    public static class AletheiaInsiderTradingService
    {
        public static async Task<SecurityTransactionHolding[]> LatestTransactionsAsync(this AletheiaService service, string issuer = null, string owner = null, int top = 20, DateTime? before = null, SecurityType? security_filter = null, TransactionType? transaction_filter = null)
        {
            //Error checking
            if (issuer == null && owner == null)
            {
                throw new Exception("The issuer or owner field must be specified.");
            }

            
            //Gather the params to add
            List<KeyValuePair<string, string>> reqparams = new List<KeyValuePair<string, string>>();
            if (issuer != null)
            {
                reqparams.Add(new KeyValuePair<string, string>("issuer", issuer));
            }
            if (owner != null)
            {
                reqparams.Add(new KeyValuePair<string, string>("", ""));
            }
            reqparams.Add(new KeyValuePair<string, string>("top", top.ToString()));
            if (before.HasValue)
            {
                reqparams.Add(new KeyValuePair<string, string>("before", before.Value.Year.ToString("0000") + before.Value.Month.ToString("00") + before.Value.Day.ToString("00")));
            }
            if (security_filter.HasValue)
            {
                if (security_filter.Value == SecurityType.NonDerivative)
                {
                    reqparams.Add(new KeyValuePair<string, string>("securitytype", "0"));
                }
                else
                {
                    reqparams.Add(new KeyValuePair<string, string>("securitytype", "1"));
                }
            }
            if (transaction_filter.HasValue)
            {
                reqparams.Add(new KeyValuePair<string, string>("transactiontype", Convert.ToInt32(transaction_filter.Value).ToString()));
            }
            
            //Assemble the url
            string req = "https://api.aletheiaapi.com/LatestTransactions?";
            foreach (KeyValuePair<string, string> kvp in reqparams)
            {
                req = req + kvp.Key + "=" + kvp.Value + "&";
            }
            req = req.Substring(0, req.Length-1); //Remove the last & symbol

            //Make the call
            HttpClient hc = new HttpClient();
            HttpRequestMessage httpreq = new HttpRequestMessage();
            httpreq.Headers.Add("key", service.ApiKey);
            httpreq.RequestUri = new Uri(req);
            httpreq.Method = HttpMethod.Get;
            HttpResponseMessage resp = await hc.SendAsync(httpreq);

            //Get the body
            string content = await resp.Content.ReadAsStringAsync();
            if (resp.StatusCode != HttpStatusCode.OK)
            {
                throw new Exception("Response from Aletheia API was " + resp.StatusCode.ToString() + ". Msg: " + content);
            }
            SecurityTransactionHolding[] holdings = JsonConvert.DeserializeObject<SecurityTransactionHolding[]>(content);
            return holdings;
        }
    
    }
}
