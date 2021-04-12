using System;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;
using TimHanewich.MicrosoftGraphHelper;
using TimHanewich.MicrosoftGraphHelper.Outlook;
using Aletheia;
using Aletheia.Engine.Cloud;

namespace Aletheia.Engine.Email
{
    public static class AletheiaEmailClient
    {

        public static async Task SendEmailVerificationMessageAsync(this AletheiaCloudClient acc, string to_email, string verif_code)
        {
            //Get the HTML content of the message
            string email_msg_template_url = "https://aletheiaapi.com/email-templates/email-verification.html";
            HttpClient hc = new HttpClient();
            HttpResponseMessage hrm = await hc.GetAsync(email_msg_template_url);
            string msg_html_content = await hrm.Content.ReadAsStringAsync();

            //Replace the placeholder with the code
            msg_html_content = msg_html_content.Replace("XXXXX", verif_code);

            //Send the email
            OutlookEmailMessage msg = new OutlookEmailMessage();
            msg.Subject = "Aletheia Verification";
            msg.Content = msg_html_content;
            msg.ContentType = OutlookEmailMessageContentType.HTML;
            msg.ToRecipients.Add(to_email);

            //Retrieve the graph auth state
            MicrosoftGraphHelper mgh = await acc.RetrieveMicrosoftGraphHelperStateAsync();

            //Update the access token if it is expired
            //I THINK i put this into the send email message method, so this isnt really necessary... but I do it anyway just to be sure.
            if (mgh.AccessTokenHasExpired())
            {
                await mgh.RefreshAccessTokenIfExpiredAsync(); //Refresh the token
                await acc.UploadMicrosoftGraphHelperStateAsync(mgh); //Upload the new state (save the new access token and refresh token)
            }
            
            //Send the email
            await mgh.SendOutlookEmailMessageAsync(msg);
        }


    }
}