using System;
using SendGrid;
using SendGrid.Helpers;
using SendGrid.Helpers.Mail;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;

namespace Aletheia.Email
{
    public class AletheiaEmailClient
    {
        //API Key
        private string SendGridApiKey = "";

        public AletheiaEmailClient(string send_grid_key)
        {
            SendGridApiKey = send_grid_key;
        }

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
            SendGridClient sgc = new SendGridClient(SendGridApiKey);
            EmailAddress from = new EmailAddress("accounts@aletheiaapi.com", "Aletheia API");
            EmailAddress to = new EmailAddress(to_email);
            SendGridMessage sgmsg = MailHelper.CreateSingleEmail(from, to, "Aletheia Email Verification", "", msg_html_content);
            await sgc.SendEmailAsync(sgmsg);
        }


    }
}