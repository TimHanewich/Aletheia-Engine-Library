using System;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Aletheia.Service.Fundamentals
{
    public static class AletheiaFundamentalsService
    {
        public static async Task<FinancialFactTrendDataPoint[]> FinancialFactTrendAsync(this AletheiaService service, FinancialFactTrendRequest request)
        {

            //Error check
            if (request.Id == null)
            {
                throw new Exception("Request ID was null.");
            }
            if (request.Id == "")
            {
                throw new Exception("Request ID was blank.");
            }

            //Core
            string req_url = "https://api.aletheiaapi.com/FinancialFactTrend";
            req_url = req_url + "?id=" + request.Id;
            req_url = req_url + "&label=" + Convert.ToInt32(request.Label).ToString();

            //optional
            if (request.PeriodType.HasValue)
            {
                req_url = req_url + "&period=" + Convert.ToInt32(request.PeriodType).ToString();
            }
            if (request.After.HasValue)
            {
                req_url = req_url + "&after=" + request.After.Value.Year.ToString("0000") + request.After.Value.Month.ToString("00") + request.After.Value.Day.ToString("00");
            }
            if (request.Before.HasValue)
            {
                req_url = req_url + "&before=" + request.Before.Value.Year.ToString("0000") + request.Before.Value.Month.ToString("00") + request.Before.Value.Day.ToString("00");
            }


            //Make the request message
            HttpClient hc = new HttpClient();
            HttpRequestMessage req = new HttpRequestMessage();
            req.Method = HttpMethod.Get;       
            req.RequestUri = new Uri(req_url);
            req.Headers.Add("key", service.ApiKey);
            
            //Call
            HttpResponseMessage resp = await hc.SendAsync(req);
            string content = await resp.Content.ReadAsStringAsync();
            if (resp.StatusCode != HttpStatusCode.OK)
            {
                throw new Exception("Failed response with code '" + resp.Content.ToString() + "'. Content: " + content);
            }

            //Get the objects
            List<FinancialFactTrendDataPoint> ToReturn = new List<FinancialFactTrendDataPoint>();
            JArray ja = JArray.Parse(content);
            foreach (JObject jo in ja)
            {
                FinancialFactTrendDataPoint dp = new FinancialFactTrendDataPoint();
                dp.PeriodStart = DateTime.Parse(jo.Property("PeriodStart").Value.ToString());
                dp.PeriodEnd = DateTime.Parse(jo.Property("PeriodEnd").Value.ToString());
                dp.Value = Convert.ToSingle(jo.Property("Value").Value.ToString());
                ToReturn.Add(dp);
            }
            
            
            return ToReturn.ToArray();
        }
    }
}