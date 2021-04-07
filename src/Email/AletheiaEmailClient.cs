using System;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;

namespace Aletheia.Email
{
    public class AletheiaEmailClient
    {

        public async Task SendEmailVerificationMessageAsync(string to_email, string verif_code)
        {
            //Get the HTML content of the message
            string email_msg_template_url = "https://aletheiaapi.com/email-templates/email-verification.html";
            HttpClient hc = new HttpClient();
            HttpResponseMessage hrm = await hc.GetAsync(email_msg_template_url);
            string msg_html_content = await hrm.Content.ReadAsStringAsync();

            //Replace the placeholder with the code
            msg_html_content = msg_html_content.Replace("XXXXX", verif_code);

            //Send the email
            
        }


    }
}