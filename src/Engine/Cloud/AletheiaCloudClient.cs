using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.IO;
using SecuritiesExchangeCommission.Edgar;
using Microsoft.Azure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Aletheia.Engine.Cloud.User;
using Aletheia.Fundamentals;
using Xbrl.FinancialStatement;
using TimHanewich.MicrosoftGraphHelper;
using Aletheia.InsiderTrading;
using Aletheia.Engine.Cloud.Webhooks;
using Aletheia.Engine.EarningsCalls;
using TheMotleyFool.Transcripts;
using Aletheia.Engine.EarningsCalls.ProcessingComponents;

namespace Aletheia.Engine.Cloud
{
    public class AletheiaCloudClient
    {
        private AletheiaCredentialPackage CredentialPackage;
        
        public AletheiaCloudClient(AletheiaCredentialPackage credential_package)
        {
            string errmsg = null;
            if (credential_package == null)
            {
                errmsg = "The supplied Aletheia Credential Package was null.";
            }
            else
            {
                if (credential_package.SqlConnectionString == null)
                {
                    errmsg = "The SQL Connection String was null.";
                }
                if (credential_package.AzureStorageConnectionString == null)
                {
                    errmsg = "The Azure Storage Connection String was null.";
                }
            }

            if (errmsg != null)
            {
                throw new Exception(errmsg);
            }

            CredentialPackage = credential_package;
            SqlCpuGovernor = null;
            SqlCpuGovernorCheckDelay = new TimeSpan(0, 0, 20); //Default check delay
        }

        #region "SQL"

        #region "Insider Trading"

        #region "SecFiling"

        public async Task<Guid[]> FindSecFilingAsync(string accession_number)
        {
            List<string> Splitter = new List<string>();
            Splitter.Add("-");
            string[] parts = accession_number.Split(Splitter.ToArray(), StringSplitOptions.None);
            Guid[] ToReturn = await FindSecFilingAsync(Convert.ToInt64(parts[0]), Convert.ToInt32(parts[1]), Convert.ToInt32(parts[2]));
            return ToReturn;
        }
        
        //Returns a GUID ID of the sec filing if it was found, null if not found (doesn't exist)
        public async Task<Guid[]> FindSecFilingAsync(long accessionP1, int accessionP2, int accessionP3)
        {
            List<Guid> ToReturn = new List<Guid>();

            string cmd = "select Id from SecFiling where AccessionP1 = " + accessionP1.ToString() + " and AccessionP2 = " + accessionP2.ToString() + " and AccessionP3 = " + accessionP3.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return ToReturn.ToArray();
            }
            else
            {
                while (dr.Read())
                {
                    ToReturn.Add(dr.GetGuid(0));
                }
                sqlcon.Close();
                return ToReturn.ToArray();
            }
        }
        
        public async Task UploadSecFilingAsync(SecFiling filing)
        {
            TableInsertHelper tih = new TableInsertHelper("SecFiling");
            tih.AddColumnValuePair("Id", filing.Id.ToString(), true);
            tih.AddColumnValuePair("FilingUrl", filing.FilingUrl, true);
            tih.AddColumnValuePair("AccessionP1", filing.AccessionP1.ToString());
            tih.AddColumnValuePair("AccessionP2", filing.AccessionP2.ToString());
            tih.AddColumnValuePair("AccessionP3", filing.AccessionP3.ToString());
            tih.AddColumnValuePair("FilingType", Convert.ToInt32(filing.FilingType).ToString());
            tih.AddColumnValuePair("ReportedOn", filing.ReportedOn.ToString(), true);
            tih.AddColumnValuePair("Issuer", filing.Issuer.ToString(), true);
            if (filing.Owner.HasValue)
            {
                tih.AddColumnValuePair("Owner", filing.Owner.ToString(), true);
            }
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<SecFiling> GetSecFilingByIdAsync(Guid id)
        {
            string cmd = "select Id, FilingUrl, AccessionP1, AccessionP2, AccessionP3, FilingType, ReportedOn, Issuer, Owner from SecFiling where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SEC Filing with Id '" + id.ToString() + "'");
            }
            dr.Read();
            SecFiling ToReturn = ExtractSecFilingFromSqlDataReader(dr);
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<SecFiling> GetSecFilingByFilingUrlAsync(string url)
        {
            string cmd = "select Id, FilingUrl, AccessionP1, AccessionP2, AccessionP3, FilingType, ReportedOn, Issuer, Owner from SecFiling where FilingUrl = '" + url + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SEC Filing with Filing URL '" + url + "'");
            }
            dr.Read();
            SecFiling ToReturn = ExtractSecFilingFromSqlDataReader(dr);
            sqlcon.Close();
            return ToReturn;
        }

        public async Task DeleteSecFilingAsync(Guid id)
        {
            string cmd = "delete from SecFiling where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task DeleteSecFilingAsync(string filing_url)
        {
            string cmd = "delete from SecFiling where FilingUrl = '" + filing_url + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        /// <summary>
        /// Deletes the SecFiling(s) for a particular filing by accession number
        /// </summary>
        public async Task DeleteSecFilingAsync(long accessionP1, int accessionP2, int accessionP3)
        {
            string cmd = "delete from SecFiling where AccessionP1 = " + accessionP1.ToString() + " and AccessionP2 = " + accessionP2.ToString() + " and AccessionP3 = " + accessionP3;
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        private SecFiling ExtractSecFilingFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SecFiling ToReturn = new SecFiling();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Filing Url
            try
            {
                ToReturn.FilingUrl = dr.GetString(dr.GetOrdinal(prefix + "FilingUrl"));
            }
            catch
            {

            }

            //AccessionP1
            try
            {
                ToReturn.AccessionP1 = dr.GetInt64(dr.GetOrdinal(prefix + "AccessionP1"));
            }
            catch
            {

            }

            //AccessionP2
            try
            {
                ToReturn.AccessionP2 = dr.GetByte(dr.GetOrdinal(prefix + "AccessionP2"));
            }
            catch
            {

            }

            //AccessionP3
            try
            {
                ToReturn.AccessionP3 = dr.GetInt32(dr.GetOrdinal(prefix + "AccessionP3"));
            }
            catch
            {

            }

            //Filing Type
            try
            {
                ToReturn.FilingType = (FilingType)dr.GetByte(dr.GetOrdinal(prefix + "FilingType"));
            }
            catch
            {

            }

            //Reported On
            try
            {
                ToReturn.ReportedOn = dr.GetDateTime(dr.GetOrdinal(prefix + "ReportedOn"));
            }
            catch
            {

            }

            //Issuer
            try
            {
                ToReturn.Issuer = dr.GetInt64(dr.GetOrdinal(prefix + "Issuer"));
            }
            catch
            {

            }

            //Owner
            try
            {
                ToReturn.Owner = dr.GetInt64(dr.GetOrdinal(prefix + "Owner"));
            }
            catch
            {

            }
            
            return ToReturn;
        }

        #endregion

        #region "SecEntity"

        public async Task<bool> SecEntityExistsAsync(long cik)
        {
            string cmd = "select count(Cik) from SecEntity where Cik = " + cik;
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task UploadSecEntityAsync(SecEntity entity)
        {
            TableInsertHelper tih = new TableInsertHelper("SecEntity");
            tih.AddColumnValuePair("Cik", entity.Cik.ToString());
            tih.AddColumnValuePair("Name", entity.Name.Trim().Replace("'", "").Replace("\"", ""), true);
            if (entity.TradingSymbol != null)
            {
                if (entity.TradingSymbol != "")
                {
                    tih.AddColumnValuePair("TradingSymbol", entity.TradingSymbol.Trim().Replace("'", "").Replace("\"", "").ToUpper(), true);
                }   
            }
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();           
        }

        public async Task<SecEntity[]> SearchSecEntitiesAsync(string term, int top = 20)
        {
            string cmd = "select top " + top.ToString() + " Cik, Name, TradingSymbol from SecEntity where Cik like '%" + term + "%' or Name like '%" + term + "%' or TradingSymbol like '%" + term + "%'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<SecEntity> ToReturn = new List<SecEntity>();
            while (dr.Read())
            {
                SecEntity sec = new SecEntity();

                //Cik
                if (dr.IsDBNull(0) == false)
                {
                    sec.Cik = dr.GetInt64(0);
                }

                //Name
                if (dr.IsDBNull(1) == false)
                {
                    sec.Name = dr.GetString(1);
                }

                //Trading Symbol
                if (dr.IsDBNull(2) == false)
                {
                    sec.TradingSymbol = dr.GetString(2);
                }

                ToReturn.Add(sec);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<SecEntity> GetSecEntityByCikAsync(long cik)
        {
            string cmd = "select Cik, Name, TradingSymbol from SecEntity where Cik = " + cik.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SecEntity with CIK '" + cik.ToString() + "'");
            }
            await dr.ReadAsync();
            SecEntity ToReturn = ExtractSecEntityFromSqlDataReader(dr);
            return ToReturn;
        }

        public async Task<SecEntity> GetSecEntityByTradingSymbolAsync(string symbol)
        {
            string cmd = "select Cik, Name from SecEntity where TradingSymbol = '" + symbol.Trim().ToUpper() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SecEntity with Trading Symbol '" + symbol + "'");
            }
            await dr.ReadAsync();
            SecEntity ToReturn = ExtractSecEntityFromSqlDataReader(dr);
            sqlcon.Close();
            ToReturn.TradingSymbol = symbol.Trim().ToUpper();
            return ToReturn;
        }

        public async Task UpdateSecEntityTradingSymbol(long cik, string symbol)
        {
            string cmd = "update SecEntity set TradingSymbol = '" + symbol.Trim().ToUpper() + "' where Cik = " + cik.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        //Gets all of the people (owners) who have securities/at one point did have securities in a company (looks at Filings that attached the two together)
        public async Task<SecEntity[]> GetAffiliatedOwnersAsync(long company)
        {
            string cmd = "select distinct Person.Cik, Person.Name, Person.TradingSymbol from SecEntity as Company inner join SecFiling as Filing on Company.Cik = Filing.Issuer inner join SecEntity as Person on Filing.Owner = Person.Cik where Company.Cik = " + company.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<SecEntity> ToReturn = new List<SecEntity>();
            while (dr.Read())
            {
                SecEntity thisentity = ExtractSecEntityFromSqlDataReader(dr);
                ToReturn.Add(thisentity);
            }
            sqlcon.Close();
            return ToReturn.ToArray();        
        }

        //Gets all of the companies that a person (owner) owns or at one point did. Looks for SecFilings with the two
        public async Task<SecEntity[]> GetAffilliatedIssuersAsync(long owner)
        {
            string cmd = "select distinct Company.Cik, Company.Name, Company.TradingSymbol from SecEntity as Person inner join SecFiling as Filing on Person.Cik = Filing.Owner inner join SecEntity as Company on Filing.Issuer = Company.Cik where Person.Cik = " + owner.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<SecEntity> ToReturn = new List<SecEntity>();
            while (dr.Read())
            {
                SecEntity ThisCo = ExtractSecEntityFromSqlDataReader(dr);
                ToReturn.Add(ThisCo);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public SecEntity ExtractSecEntityFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SecEntity ToReturn = new SecEntity();

            //Cik
            try
            {
                ToReturn.Cik = dr.GetInt64(dr.GetOrdinal(prefix + "Cik"));
            }
            catch
            {

            }

            //Name
            try
            {
                ToReturn.Name = dr.GetString(dr.GetOrdinal(prefix + "Name"));
            }
            catch
            {

            }

            //Trading Symbol
            try
            {
                ToReturn.TradingSymbol = dr.GetString(dr.GetOrdinal(prefix + "TradingSymbol"));
            }
            catch
            {

            }

            return ToReturn;
        }

        #endregion

        #region "SecurityTransactionHolding"

        private string SecurityTransactionHoldingColumns = "SecurityTransactionHolding.Id, SecurityTransactionHolding.FromFiling, SecurityTransactionHolding.EntryType, SecurityTransactionHolding.AcquiredDisposed, SecurityTransactionHolding.Quantity, SecurityTransactionHolding.PricePerSecurity, SecurityTransactionHolding.TransactionDate, SecurityTransactionHolding.TransactionCode, SecurityTransactionHolding.QuantityOwnedFollowingTransaction, SecurityTransactionHolding.DirectIndirect, SecurityTransactionHolding.SecurityTitle, SecurityTransactionHolding.SecurityType, SecurityTransactionHolding.ConversionOrExcercisePrice, SecurityTransactionHolding.ExcercisableDate, SecurityTransactionHolding.ExpirationDate, SecurityTransactionHolding.UnderlyingSecurityTitle, SecurityTransactionHolding.UnderlyingSecurityQuantity";

        public async Task UploadSecurityTransactionHoldingAsync(SecurityTransactionHolding sth)
        {
            TableInsertHelper tih = new TableInsertHelper("SecurityTransactionHolding");
            tih.AddColumnValuePair("Id", sth.Id.ToString(), true);
            tih.AddColumnValuePair("FromFiling", sth.FromFiling.ToString(), true);
            
            //Entry type
            if (sth.EntryType == TransactionHoldingEntryType.Transaction)
            {
                tih.AddColumnValuePair("EntryType", "0");
            }
            else if (sth.EntryType == TransactionHoldingEntryType.Holding)
            {
                tih.AddColumnValuePair("EntryType", "1");
            }

            //Quantity owned following transaction
            tih.AddColumnValuePair("QuantityOwnedFollowingTransaction", sth.QuantityOwnedFollowingTransaction.ToString());

            //Direct Indirect ownership
            if (sth.DirectIndirect == DirectIndirect.Direct)
            {
                tih.AddColumnValuePair("DirectIndirect", "0");
            }
            else if (sth.DirectIndirect == DirectIndirect.Indirect)
            {
                tih.AddColumnValuePair("DirectIndirect", "1");
            }

            //Security Title
            if (sth.SecurityTitle != null)
            {
                if (sth.SecurityTitle != "")
                {
                    tih.AddColumnValuePair("SecurityTitle", sth.SecurityTitle.Trim().Replace("'", "").Replace("\"", ""), true);
                }
            }

            //Security type
            if (sth.SecurityType == SecurityType.NonDerivative)
            {
                tih.AddColumnValuePair("SecurityType", "0");
            }
            else if (sth.SecurityType == SecurityType.Derivative)
            {
                tih.AddColumnValuePair("SecurityType", "1");
            }

            //If it is not a holding, it is a transaction. So add in the transaction related details
            if (sth.EntryType != TransactionHoldingEntryType.Holding)
            {
                //Acquired disposed
                if (sth.AcquiredDisposed.HasValue)
                {
                    if (sth.AcquiredDisposed == Aletheia.InsiderTrading.AcquiredDisposed.Acquired)
                    {
                        tih.AddColumnValuePair("AcquiredDisposed", "0");
                    }
                    else if (sth.AcquiredDisposed == Aletheia.InsiderTrading.AcquiredDisposed.Disposed)
                    {
                        tih.AddColumnValuePair("AcquiredDisposed", "1");
                }
                }
                
                //Quantity
                if (sth.Quantity.HasValue)
                {
                    tih.AddColumnValuePair("Quantity", sth.Quantity.ToString());
                }
                
                //Price per security
                if (sth.PricePerSecurity.HasValue)
                {
                    tih.AddColumnValuePair("PricePerSecurity", sth.PricePerSecurity.ToString());
                }
                
                //Transaction Date
                if (sth.TransactionDate.HasValue)
                {
                    tih.AddColumnValuePair("TransactionDate", sth.TransactionDate.ToString(), true);
                }
                
                //Transaction Code
                if (sth.TransactionCode.HasValue)
                {
                    tih.AddColumnValuePair("TransactionCode", Convert.ToInt32(sth.TransactionCode).ToString());
                }
            }

            //If the security type is derivative, add the derivative fields
            if (sth.SecurityType == SecurityType.Derivative)
            {
                //Conversion or Excercise Price
                if (sth.ConversionOrExercisePrice.HasValue)
                {
                    tih.AddColumnValuePair("ConversionOrExcercisePrice", sth.ConversionOrExercisePrice.ToString());
                }
                
                //ExcercisableDate
                if (sth.ExercisableDate.HasValue)
                {
                    tih.AddColumnValuePair("ExcercisableDate", sth.ExercisableDate.ToString(), true);
                }
                
                //Expiration Date
                if (sth.ExpirationDate.HasValue)
                {
                    tih.AddColumnValuePair("ExpirationDate", sth.ExpirationDate.ToString(), true);
                }
                
                //Underlying Security Title
                if (sth.UnderlyingSecurityTitle != null)
                {
                    if (sth.UnderlyingSecurityTitle != "")
                    {
                        tih.AddColumnValuePair("UnderlyingSecurityTitle", sth.UnderlyingSecurityTitle.Trim().Replace("'", "").Replace("\"", ""), true);
                    }
                }

                //Underlying Security Quantity
                if (sth.UnderlyingSecurityQuantity.HasValue)
                {
                    tih.AddColumnValuePair("UnderlyingSecurityQuantity", sth.UnderlyingSecurityQuantity.ToString());
                }
            }

            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();           
        }

        public async Task<SecurityTransactionHolding[]> GetSecurityTransactionHoldingsAsync(long? issuer_cik = null, long? owner_cik = null, int top = 10,  DateTime? before = null, SecurityType? security_type = null, TransactionType? transaction_type = null, bool cascade = false)
        {
            //Check that at least the issuer or owner CIK has a value
            bool HaveAtLeastOneCik = false;
            if (issuer_cik != null)
            {
                HaveAtLeastOneCik = true;
            }
            if (owner_cik != null)
            {
                HaveAtLeastOneCik = true;
            }
            if (HaveAtLeastOneCik == false)
            {
                throw new Exception("Both the Issuer and Owner CIK were null!");
            }
            
            //Establish top
            string part_tops = "select top " + top.ToString();

            //Establish columns
            string part_columns = SecurityTransactionHoldingColumns;
            if (cascade) //If it is asking for Cascade, throw in the SecFilign details as well.
            {
                //SecFiling
                part_columns = part_columns + ", SecFiling.Id as SecFiling_Id, SecFiling.FilingUrl as SecFiling_FilingUrl, SecFiling.AccessionP1 as SecFiling_AccessionP1, SecFiling.AccessionP2 as SecFiling_AccessionP2, SecFiling.AccessionP3 as SecFiling_AccessionP3, SecFiling.FilingType as SecFiling_FilingType, SecFiling.ReportedOn as SecFiling_ReportedOn, SecFiling.Issuer as SecFiling_Issuer, SecFiling.Owner as SecFiling_Owner";
            
                //If an issuer is specified, add that
                //if (issuer_cik.HasValue)
                //{
                    part_columns = part_columns + ", EntIssuer.Cik as Issuer_Cik, EntIssuer.Name as Issuer_Name, EntIssuer.TradingSymbol as Issuer_TradingSymbol";
                //}
                
                //If an owner is specified, add that
                //if (owner_cik.HasValue)
                //{
                    part_columns = part_columns + ", EntOwner.Cik as Owner_Cik, EntOwner.Name as Owner_Name, EntOwner.TradingSymbol as Owner_TradingSymbol";
                //}
            }
  
            //Inner join with SecFiling
            string part_join_SecFiling = "inner join SecFiling on SecurityTransactionHolding.FromFiling = SecFiling.Id";

            //Inner join with the Issuer and Owner (SecEntity fields if asked to Cascade)
            string part_join_SecEntity_Issuer = "";
            string part_join_SecEntity_Owner = "";
            if (cascade)
            {
                part_join_SecEntity_Issuer = "inner join SecEntity as EntIssuer on SecFiling.Issuer = EntIssuer.Cik";
                part_join_SecEntity_Owner = "inner join SecEntity as EntOwner on SecFiling.Owner = EntOwner.Cik";
            }

            #region "where clause"

            string part_where = "";

            List<string> parts_where = new List<string>();

            //SecEntity - Issuer
            if (issuer_cik.HasValue)
            {
                parts_where.Add("SecFiling.Issuer = " + issuer_cik.ToString());
            }

            //SecEntity - Owner
            if (owner_cik.HasValue)
            {
                parts_where.Add("SecFiling.Owner = " + owner_cik.ToString());
            }

            //Before
            if (before.HasValue)
            {
                parts_where.Add("TransactionDate < '" + before.Value.ToString() + "'");
            }

            //SecurityType
            if (security_type.HasValue)
            {
                parts_where.Add("SecurityType = " + Convert.ToInt32(security_type.Value).ToString());
            }

            //TransactionCode
            if (transaction_type.HasValue)
            {
                parts_where.Add("TransactionCode = " + Convert.ToInt32(transaction_type.Value).ToString());
            }

            //Compile to one big where string
            part_where = "where ";
            foreach (string s in parts_where)
            {
                part_where = part_where + s + " and ";
            }
            part_where = part_where.Substring(0, part_where.Length - 5); //remove the last hanging and

            #endregion
            
            //Order by
            string part_orderby = "order by SecurityTransactionHolding.TransactionDate desc";

            //COMPILE THE COMMAND!
            string cmd = "";
            cmd = part_tops + " " + part_columns + " from SecurityTransactionHolding " + part_join_SecFiling + " " + part_join_SecEntity_Issuer + " " + part_join_SecEntity_Owner + " " + part_where + " " + part_orderby;


            //Call
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<SecurityTransactionHolding> ToReturn = new List<SecurityTransactionHolding>();
            while (dr.Read())
            {
                SecurityTransactionHolding sth = ExtractSecurityTransactionHoldingFromSqlDataReader(dr);

                if (cascade)
                {
                    //Get the filing
                    SecFiling filing = ExtractSecFilingFromSqlDataReader(dr, "SecFiling_");
                    sth._FromFiling = filing;

                    //Get the issuer
                    SecEntity issuer = ExtractSecEntityFromSqlDataReader(dr, "Issuer_");
                    sth._FromFiling._Issuer = issuer;

                    //Get the owner
                    SecEntity owner = ExtractSecEntityFromSqlDataReader(dr, "Owner_");
                    sth._FromFiling._Owner = owner;
                }

                ToReturn.Add(sth);
            }

            sqlcon.Close();

            return ToReturn.ToArray();            
        }

        public async Task<int> CountSecurityTransactionHoldingsFromSecFilingAsync(Guid sec_filing)
        {
            string cmd = "select count(Id) from SecurityTransactionHolding where FromFiling = '" + sec_filing + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int ToReturn = dr.GetInt32(0);
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<int> CountSecurityTransactionHoldingsFromSecFilingAsync(string filing_url)
        {
            string cmd = "select count(SecurityTransactionHolding.Id) from SecurityTransactionHolding inner join SecFiling on SecurityTransactionHolding.FromFiling = SecFiling.Id where SecFiling.FilingUrl = '" + filing_url + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int ToReturn = dr.GetInt32(0);
            sqlcon.Close();
            return ToReturn;
        }

        public SecurityTransactionHolding ExtractSecurityTransactionHoldingFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SecurityTransactionHolding ToReturn = new SecurityTransactionHolding();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //FromFiling
            try
            {
                ToReturn.FromFiling = dr.GetGuid(dr.GetOrdinal(prefix + "FromFiling"));
            }
            catch
            {

            }

            //EntryType
            try
            {
                bool val = dr.GetBoolean(dr.GetOrdinal(prefix + "EntryType"));
                if (val)
                {
                    ToReturn.EntryType = TransactionHoldingEntryType.Holding;
                }
                else
                {
                    ToReturn.EntryType = TransactionHoldingEntryType.Transaction;
                }
            }
            catch
            {

            }

            //AcquiredDisposed
            try
            {
                bool val = dr.GetBoolean(dr.GetOrdinal(prefix + "AcquiredDisposed"));
                if (val)
                {
                    ToReturn.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Disposed;
                }
                else
                {
                    ToReturn.AcquiredDisposed = Aletheia.InsiderTrading.AcquiredDisposed.Acquired;
                }
            }
            catch
            {

            }

            //Quantity
            try
            {
                ToReturn.Quantity = dr.GetFloat(dr.GetOrdinal(prefix + "Quantity"));
            }
            catch
            {

            }

            //Price Per security
            try
            {
                ToReturn.PricePerSecurity = dr.GetFloat(dr.GetOrdinal(prefix + "PricePerSecurity"));
            }
            catch
            {

            }
            
            //Transaction Date
            try
            {
                ToReturn.TransactionDate = dr.GetDateTime(dr.GetOrdinal(prefix + "TransactionDate"));
            }
            catch
            {

            }

            //Transaction Code
            try
            {
                ToReturn.TransactionCode = (TransactionType)dr.GetByte(dr.GetOrdinal(prefix + "TransactionCode"));
            }
            catch
            {

            }

            //TQuantityOwnedFollowingTransaction
            try
            {
                ToReturn.QuantityOwnedFollowingTransaction = dr.GetFloat(dr.GetOrdinal(prefix + "QuantityOwnedFollowingTransaction"));
            }
            catch
            {

            }

            //DirectIndirect
            try
            {
                bool val = dr.GetBoolean(dr.GetOrdinal(prefix + "DirectIndirect"));
                if (val)
                {
                    ToReturn.DirectIndirect = DirectIndirect.Indirect;
                }
                else
                {
                    ToReturn.DirectIndirect = DirectIndirect.Direct;
                }
            }
            catch
            {

            }

            //SecurityTitle
            try
            {
                ToReturn.SecurityTitle = dr.GetString(dr.GetOrdinal(prefix + "SecurityTitle"));
            }
            catch
            {

            }

            //SecurityType
            try
            {
                bool val = dr.GetBoolean(dr.GetOrdinal(prefix + "SecurityType"));
                if (val)
                {
                    ToReturn.SecurityType = SecurityType.Derivative;
                }
                else
                {
                    ToReturn.SecurityType = SecurityType.NonDerivative;
                }
            }
            catch
            {

            }

            //ConversionOrExcercisePrice
            try
            {
                ToReturn.ConversionOrExercisePrice = dr.GetFloat(dr.GetOrdinal(prefix + "ConversionOrExercisePrice"));
            }
            catch
            {

            }

            //ExcercisbleDate
            try
            {
                ToReturn.ExercisableDate = dr.GetDateTime(dr.GetOrdinal(prefix + "ExercisableDate"));
            }
            catch
            {

            }

            //ExpirationDate
            try
            {
                ToReturn.ExpirationDate = dr.GetDateTime(dr.GetOrdinal(prefix + "ExpirationDate"));
            }
            catch
            {

            }

            //Underlying security tite
            try
            {
                ToReturn.UnderlyingSecurityTitle = dr.GetString(dr.GetOrdinal(prefix + "UnderlyingSecurityTitle"));
            }
            catch
            {

            }

            //UnderlyingSecurityQuantity
            try
            {
                ToReturn.UnderlyingSecurityQuantity = dr.GetFloat(dr.GetOrdinal(prefix + "UnderlyingSecurityQuantity"));
            }
            catch
            {

            }
            

            return ToReturn;
        }

        public async Task<Guid[]> GetSecurityTransactionHoldingIdsFromFilingAsync(Guid sec_filing_id)
        {
            string cmd = "select Id from SecurityTransactionHolding where FromFiling = '" + sec_filing_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            
            List<Guid> ToReturn = new List<Guid>();
            while (dr.Read())
            {
                ToReturn.Add(dr.GetGuid(0));
            }

            sqlcon.Close();
            return ToReturn.ToArray();
        }
        
        public async Task DeleteSecurityTransactionHoldingAsync(Guid id)
        {
            string cmd = "delete from SecurityTransactionHolding where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task DeleteSecurityTransactionHoldingsFromFilingAsync(Guid sec_filing_id)
        {
            string sql = "delete from SecurityTransactionHolding where FromFiling = '" + sec_filing_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(sql, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        #endregion

        #region "HeldOfficerPosition"

        public async Task<Guid?> FindHeldOfficerPositionAsync(long company, long officer, string position_title)
        {
            string cmd = "select Id from HeldOfficerPosition where Company = " + company.ToString() + " and Officer = " + officer.ToString() + " and PositionTitle = '" + position_title + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            else
            {
                await dr.ReadAsync();
                Guid ToReturn = dr.GetGuid(0);
                sqlcon.Close();
                return ToReturn;
            }
        }

        public async Task UploadHeldOfficerPositionAsync(HeldOfficerPosition hop)
        {
            TableInsertHelper tih = new TableInsertHelper("HeldOfficerPosition");
            tih.AddColumnValuePair("Id", hop.Id.ToString(), true);
            tih.AddColumnValuePair("Officer", hop.Officer.ToString());
            tih.AddColumnValuePair("Company", hop.Company.ToString());
            tih.AddColumnValuePair("PositionTitle", hop.PositionTitle.Trim().Replace("'", "").Replace("\"", ""), true);
            tih.AddColumnValuePair("ObservedOn", hop.ObservedOn.ToString(), true);
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task  DeleteHeldOfficerPositionsFromFilingAsync(Guid sec_filing_id)
        {
            string sql = "delete from HeldOfficerPosition where ObservedOn = '" + sec_filing_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(sql, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        #endregion

        #endregion

        #region "Webhook subscription tables"

        public async Task UploadWebhookSubscriptionAsync(WebhookSubscription sub)
        {
            //Make sure a webhook subscription does not already exist with this endpoint
            bool AlreadyExists = await WebhookSubscriptionExistsAsync(sub.Endpoint);
            if (AlreadyExists)
            {
                throw new Exception("A webhook has already been registered on endpoint '" + sub.Endpoint + "'");
            }

            //If the sub does not have an ID, give it one
            if (sub.Id == Guid.Empty)
            {
                sub.Id = Guid.NewGuid();
            }

            string cmd = "insert into WebhookSubscription (Id, Endpoint, AddedAtUtc, RegisteredToKey) values ('" + sub.Id.ToString() + "', '" + sub.Endpoint + "', '" + sub.AddedAtUtc.ToString() + "', '" + sub.RegisteredToKey + "')";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task UploadNewFilingsWebhookSubscriptionAsync(NewFilingsWebhookSubscription subscription)
        {
            //First check to make sure a table for this subscription does not already exist
            bool AlreadyExistsForSubscription = await NewFilingsWebhookSubscriptionExistsAsync(subscription.Subscription);
            if (AlreadyExistsForSubscription)
            {
                throw new Exception("New Filings details already exist for that specified webhook.");
            }

            string cmd = "insert into NewFilingsWebhookSubscription (Id, Subscription) values ('" + subscription.Id.ToString() + "', '" + subscription.Subscription.ToString() + "')";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task UploadInsiderTradingWebhookSubscriptionAsync(InsiderTradingWebhookSubscription subscription)
        {
            //First make sure InsiderTradingWebhookSubscription details do not alraedy exists for this specified endpoint
            bool AlreadyExists = await InsiderTradingWebhookSubscriptionExistsAsync(subscription.Subscription);
            if (AlreadyExists)
            {
                throw new Exception("An InsiderTradingWebhookSubscription record with details already exists for webhook '" + subscription.Subscription + "'");
            }

            TableInsertHelper tih = new TableInsertHelper("InsiderTradingWebhookSubscription");
            tih.AddColumnValuePair("Id", subscription.Id.ToString(), true);
            tih.AddColumnValuePair("Subscription", subscription.Subscription.ToString(), true);
            if (subscription.IssuerCik.HasValue)
            {
                tih.AddColumnValuePair("IssuerCik", subscription.IssuerCik.Value.ToString(), false);
            }
            if (subscription.OwnerCik.HasValue)
            {
                tih.AddColumnValuePair("OwnerCik", subscription.OwnerCik.Value.ToString(), false);
            }
            if (subscription.SecurityType.HasValue)
            {
                tih.AddColumnValuePair("SecurityType", Convert.ToInt32(subscription.SecurityType.Value).ToString(), false);
            }
            if (subscription.TransactionType.HasValue)
            {
                tih.AddColumnValuePair("TransactionType", Convert.ToInt32(subscription.TransactionType.Value).ToString(), false);
            }

            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<string[]> GetNewFilingsWebhookSubscriptionEndpointsAsync()
        {
            string cmd = "select distinct Endpoint from WebhookSubscription inner join NewFilingsWebhookSubscription on WebhookSubscription.Id = NewFilingsWebhookSubscription.Subscription";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<string> ToReturn = new List<string>();
            while (dr.Read())
            {                
                if (dr.IsDBNull(0) == false)
                {
                    ToReturn.Add(dr.GetString(0));
                }
            }

            sqlcon.Close();

            return ToReturn.ToArray();
        }

        //Gets the WebhookSubscription details (what endpoints to call, the subscribed to key, etc) for subscriptions with particular details
        public async Task<WebhookSubscription[]> GetQualifyingNewFilingsWebhookSubscriptionsAsync()
        {
            string cmd = "select WebhookSubscription.Id, WebhookSubscription.Endpoint, WebhookSubscription.AddedAtUtc, WebhookSubscription.RegisteredToKey from WebhookSubscription inner join NewFilingsWebhookSubscription on WebhookSubscription.Id = NewFilingsWebhookSubscription.Subscription";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<WebhookSubscription> ToReturn = new List<WebhookSubscription>();
            while (dr.Read())
            {
                ToReturn.Add(ExtractWebhookSubscriptionFromSqlDataReader(dr));
            }
            sqlcon.Close();

            return ToReturn.ToArray();
        }

        public async Task<string[]> GetQualifyingInsiderTradingWebhookSubscriptionEndpointsAsync(InsiderTradingWebhookSubscription template)
        {
            List<string> CmdStack = new List<string>();
            CmdStack.Add("select Endpoint from WebhookSubscription");
            CmdStack.Add("inner join InsiderTradingWebhookSubscription on WebhookSubscription.Id = InsiderTradingWebhookSubscription.Subscription");
            
            //Where clauses?
            List<string> WhereClause = new List<string>();
            if (template.IssuerCik.HasValue)
            {
                WhereClause.Add("(IssuerCik = " + template.IssuerCik.Value.ToString() + " or IssuerCik is null)");
            }
            if (template.OwnerCik.HasValue)
            {
                WhereClause.Add("(OwnerCik = " + template.OwnerCik.Value.ToString() + " or OwnerCik is null)");
            }
            if (template.SecurityType.HasValue)
            {
                WhereClause.Add("(SecurityType = " + Convert.ToInt32(template.SecurityType.Value).ToString() + " or SecurityType is null)");
            }
            if (template.TransactionType.HasValue)
            {
                WhereClause.Add("(TransactionType = " + Convert.ToInt32(template.TransactionType).ToString() + " or TransactionType is null)");
            }
            
            //If there is at least one where clause, then add them in
            if (WhereClause.Count > 0)
            {
                CmdStack.Add("where");
                foreach (string s in WhereClause)
                {
                    CmdStack.Add(s);
                    CmdStack.Add("and");
                }
                CmdStack.RemoveAt(CmdStack.Count-1); //Remove the last one because it will be a trailing 'and' with the above code.
            }
            
            //Assemble into one big command
            string cmd = "";
            foreach (string s in CmdStack)
            {
                cmd = cmd + s + Environment.NewLine;
            }

            //Call it
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            //Extract and return
            List<string> ToReturn = new List<string>();
            while (dr.Read())
            {
                ToReturn.Add(dr.GetString(0));
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<WebhookSubscription[]> GetQualifyingInsiderTradingWebhookSubscriptionsAsync(InsiderTradingWebhookSubscription template)
        {
            List<string> CmdStack = new List<string>();
            CmdStack.Add("select WebhookSubscription.Id, WebhookSubscription.Endpoint, WebhookSubscription.AddedAtUtc, WebhookSubscription.RegisteredToKey from WebhookSubscription");
            CmdStack.Add("inner join InsiderTradingWebhookSubscription on WebhookSubscription.Id = InsiderTradingWebhookSubscription.Subscription");
            
            //Where clauses?
            List<string> WhereClause = new List<string>();
            if (template.IssuerCik.HasValue)
            {
                WhereClause.Add("(IssuerCik = " + template.IssuerCik.Value.ToString() + " or IssuerCik is null)");
            }
            if (template.OwnerCik.HasValue)
            {
                WhereClause.Add("(OwnerCik = " + template.OwnerCik.Value.ToString() + " or OwnerCik is null)");
            }
            if (template.SecurityType.HasValue)
            {
                WhereClause.Add("(SecurityType = " + Convert.ToInt32(template.SecurityType.Value).ToString() + " or SecurityType is null)");
            }
            if (template.TransactionType.HasValue)
            {
                WhereClause.Add("(TransactionType = " + Convert.ToInt32(template.TransactionType).ToString() + " or TransactionType is null)");
            }
            
            //If there is at least one where clause, then add them in
            if (WhereClause.Count > 0)
            {
                CmdStack.Add("where");
                foreach (string s in WhereClause)
                {
                    CmdStack.Add(s);
                    CmdStack.Add("and");
                }
                CmdStack.RemoveAt(CmdStack.Count-1); //Remove the last one because it will be a trailing 'and' with the above code.
            }
            
            //Assemble into one big command
            string cmd = "";
            foreach (string s in CmdStack)
            {
                cmd = cmd + s + Environment.NewLine;
            }

            //Call it
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            //Extract and return
            List<WebhookSubscription> ToReturn = new List<WebhookSubscription>();
            while (dr.Read())
            {
                ToReturn.Add(ExtractWebhookSubscriptionFromSqlDataReader(dr, ""));
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        //This will unsubscribe from any webhook related table (it will check all of them)
        public async Task UnsubscribeWebhookAsync(string endpoint)
        {
            ///Delete from the NewFilingsWebhookSubscription table (if this is a new filings webhook)
            await ExecuteNonQueryAsync("delete nfws from NewFilingsWebhookSubscription nfws inner join WebhookSubscription on nfws.Subscription = WebhookSubscription.Id where WebhookSubscription.Endpoint = '" + endpoint + "'");
        
            //Delete from the InsiderTradingWebhookSubscription (if this is an insider trading webhook)
            await ExecuteNonQueryAsync("delete itws from InsiderTradingWebhookSubscription itws inner join WebhookSubscription on itws.Subscription = WebhookSubscription.Id where WebhookSubscription.Endpoint = '" + endpoint + "'");
        
            //Delete from the webhook subscription itself
            await ExecuteNonQueryAsync("delete from WebhookSubscription where Endpoint = '" + endpoint + "'");
        }
        
        public async Task UnsubscribeWebhookAsync(Guid hook_subscription_id)
        {
            ///Delete from the NewFilingsWebhookSubscription table (if this is a new filings webhook)
            await ExecuteNonQueryAsync("delete nfws from NewFilingsWebhookSubscription nfws inner join WebhookSubscription on nfws.Subscription = WebhookSubscription.Id where WebhookSubscription.Id = '" + hook_subscription_id.ToString() + "'");
        
            //Delete from the InsiderTradingWebhookSubscription (if this is an insider trading webhook)
            await ExecuteNonQueryAsync("delete itws from InsiderTradingWebhookSubscription itws inner join WebhookSubscription on itws.Subscription = WebhookSubscription.Id where WebhookSubscription.Id = '" + hook_subscription_id.ToString() + "'");
        
            //Delete from the webhook subscription itself
            await ExecuteNonQueryAsync("delete from WebhookSubscription where Id = '" + hook_subscription_id.ToString() + "'");
        }

        public async Task<bool> WebhookSubscriptionExistsAsync(string endpoint)
        {
            string cmd = "select count(Endpoint) from WebhookSubscription where Endpoint = '" + endpoint + "'";
            int val = await CountSqlCommandAsync(cmd);
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        //Will check if a webhook from the WebhookSubscription table has a corresponding NewFilings detail in the NewFilings table
        public async Task<bool> NewFilingsWebhookSubscriptionExistsAsync(Guid for_webhook)
        {
            string cmd = "select count(Subscription) from NewFilingsWebhookSubscription where Subscription = '" + for_webhook.ToString() + "'";
            int val = await CountSqlCommandAsync(cmd);
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task<bool> InsiderTradingWebhookSubscriptionExistsAsync(Guid for_webhook)
        {
            string cmd = "select count(Subscription) from InsiderTradingWebhookSubscription where Subscription = '" + for_webhook.ToString() + "'";
            int val = await CountSqlCommandAsync(cmd);
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public WebhookSubscription ExtractWebhookSubscriptionFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            WebhookSubscription ToReturn = new WebhookSubscription();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Endpoint
            try
            {
                ToReturn.Endpoint = dr.GetString(dr.GetOrdinal(prefix + "Endpoint"));
            }
            catch
            {

            }

            //AddedAtUtc
            try
            {
                ToReturn.AddedAtUtc = dr.GetDateTime(dr.GetOrdinal(prefix + "AddedAtUtc"));
            }
            catch
            {

            }

            //RegisteredToKey
            try
            {
                ToReturn.RegisteredToKey = dr.GetGuid(dr.GetOrdinal(prefix + "RegisteredToKey"));
            }
            catch
            {

            }

            return ToReturn;
        }

        #endregion

        #region "User-related tables"

        //EmailVerificationCodePair

        public async Task<bool> EmailVerificationCodePairExistsAsync(string email)
        {
            string cmd = "select count(Email) from EmailVerificationCodePair where Email = '" + email + "'";
            int val = await CountSqlCommandAsync(cmd);
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task UploadEmailVerificationCodePairAsync(EmailVerificationCodePair evcp)
        {
            string cmd = "insert into EmailVerificationCodePair (Email, Code, StartedAtUtc) values (";
            cmd = cmd + "'" + evcp.Email + "',";
            cmd = cmd + "'" + evcp.Code + "',";
            cmd = cmd + "'" + evcp.StartedAtUtc.ToString() + "'";
            cmd = cmd + ")";

            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task DeleteEmailVerificationCodePairAsync(string email)
        {
            string cmd = "delete from EmailVerificationCodePair where Email = '" + email + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<bool> EmailVerificationCodeCorrectAsync(string email, string code)
        {
            string cmd = "select Email, Code from EmailVerificationCodePair where Email = '" + email + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("An email verification for email '" + email + "' is not currently underway.");
            }
            
            string correct_code = "";
            dr.Read();
            if (dr.IsDBNull(1) == false)
            {
                correct_code = dr.GetString(1);
            }


            sqlcon.Close();
            if (code == correct_code)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        //USER ACCOUNTS

        public async Task<bool> UserAccountWithUsernameExistsAsync(string username)
        {
            string cmd = "select count(Id) from UserAccount where Username = '" + username + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            dr.Read();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task<bool> UserAccountWithEmailExistsAsync(string email)
        {
            string cmd = "select count(Id) from UserAccount where Email = '" + email + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            dr.Read();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task UploadUserAccountAsync(AletheiaUserAccount account)
        {

            //First validate the username and password
            if (AletheiaUserAccount.UsernameValid(account.Username) == false)
            {
                throw new Exception("Username '" + account.Username + "' uses disallowed characters.");
            }
            if (AletheiaUserAccount.PasswordValid(account.Password) == false)
            {
                throw new Exception("Provided password was not valid as it used disallowed characters.");
            }

            //First check if a user account with this username already exists
            bool alreadyexists = await UserAccountWithUsernameExistsAsync(account.Username);
            if (alreadyexists)
            {
                throw new Exception("User with username '" + account.Username + "' already exists.");
            }

            //Check if a user account with this email already exists (only 1 account per email)
            alreadyexists = await UserAccountWithEmailExistsAsync(account.Email);
            if (alreadyexists)
            {
                throw new Exception("User with email '" + account.Email + "' already exists.");
            }

            //Plug in an ID if one does not already exist
            if (account.Id == Guid.Empty)
            {
                account.Id = Guid.NewGuid();
            }

            string cmd = "insert into UserAccount (Id, Username, Password, Email, CreatedAtUtc) values ('" + account.Id.ToString() + "', '" + account.Username + "', '" + account.Password + "', '" + account.Email + "', '" + account.CreatedAtUtc.ToString() + "')";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<AletheiaUserAccount> GetUserAccountByUsernameAsync(string username)
        {
            string cmd = "select Id, Username, Password, Email, CreatedAtUtc from UserAccount where Username = '" + username + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find user account with username '" + username + "'.");
            }

            //Return the first one (assume there shouldn't be multiple with the same username... will need to enforce this.
            dr.Read();

            AletheiaUserAccount ToReturn = ExtractAletheiaUserAccount(dr);

            sqlcon.Close();
            return ToReturn;
        }

        public async Task<AletheiaUserAccount> GetUserAccountByIdAsync(Guid id)
        {
            string cmd = "select Username, Password, Email, CreatedAtUtc from UserAccount where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find user account with Id '" + id.ToString() + "'.");
            }

            //Return the first one (assume there shouldn't be multiple with the same username... will need to enforce this.
            dr.Read();
            AletheiaUserAccount ToReturn = ExtractAletheiaUserAccount(dr);
            ToReturn.Id = id;     
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<Guid> GetUserIdByUsernameAsync(string username)
        {
            string cmd = "select Id from UserAccount where Username = '" + username + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find user account with username '" + username + "'");
            }
            await dr.ReadAsync();
            Guid ToReturn = dr.GetGuid(0);
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<int> CountUserAccountsAsync()
        {
            string cmd = "select Count(Id) from UserAccount";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<AletheiaUserAccount> WhoMadeApiCallAsync(Guid call_id)
        {
            string cmd = "select UserAccount.Id as Id, UserAccount.Username as Username, UserAccount.Password as Password, UserAccount.Email as Email, UserAccount.CreatedAtUtc as CreatedAtUtc from UserAccount inner join ApiKey on UserAccount.Id = ApiKey.RegisteredTo inner join ApiCall on ApiKey.Token = ApiCall.ConsumedKey where ApiCall.Id = '" + call_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find API call with ID '" + call_id.ToString() + "'");
            }

            //Assuming it is only 1 user obviously here.

            dr.Read();
            AletheiaUserAccount ToReturn = ExtractAletheiaUserAccount(dr);
            sqlcon.Close();
            return ToReturn;
        }

        public AletheiaUserAccount ExtractAletheiaUserAccount(SqlDataReader dr, string prefix = "")
        {
            AletheiaUserAccount ToReturn = new AletheiaUserAccount();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Username
            try
            {
                ToReturn.Username = dr.GetString(dr.GetOrdinal(prefix + "Username"));
            }
            catch
            {

            }

            //Password
            try
            {
                ToReturn.Password = dr.GetString(dr.GetOrdinal(prefix + "Password"));
            }
            catch
            {

            }

            //Email
            try
            {
                ToReturn.Email = dr.GetString(dr.GetOrdinal(prefix + "Email"));
            }
            catch
            {

            }

            //CreatedAtUtc
            try
            {
                ToReturn.CreatedAtUtc = dr.GetDateTime(dr.GetOrdinal(prefix + "CreatedAtUtc"));
            }
            catch
            {

            }

            return ToReturn;
        }

        // API Keys
        
        public async Task UploadApiKeyAsync(AletheiaApiKey apikey, Guid? register_to_user_id = null)
        {
            string cmd = "";
            if (register_to_user_id == null)
            {
                cmd = "insert into ApiKey (Token, CreatedAtUtc) values ('" + apikey.Token.ToString() + "', '" + apikey.CreatedAtUtc + "')";
            }
            else
            {
                cmd = "insert into ApiKey (Token, CreatedAtUtc, RegisteredTo) values ('" + apikey.Token.ToString() + "', '" + apikey.CreatedAtUtc + "', '" + register_to_user_id.Value.ToString() + "')";
            }

            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }   

        public async Task<AletheiaApiKey[]> GetUsersApiKeysAsync(Guid user_id)
        {
            string cmd = "select Token, CreatedAtUtc from ApiKey where RegisteredTo = '" + user_id + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<AletheiaApiKey> ToReturn = new List<AletheiaApiKey>();
            while (dr.Read())
            {
                AletheiaApiKey thiskey = new AletheiaApiKey();

                if (dr.IsDBNull(0) == false)
                {
                    thiskey.Token = dr.GetGuid(0);
                }

                if (dr.IsDBNull(1) == false)
                {
                    thiskey.CreatedAtUtc = dr.GetDateTime(1);
                }

                ToReturn.Add(thiskey);
            }

            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<AletheiaApiKey[]> GetUsersApiKeysAsync(string username)
        {
            string cmd = "select ApiKey.Token, ApiKey.CreatedAtUtc from ApiKey inner join UserAccount on RegisteredTo = Id where UserAccount.Username = '" + username + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<AletheiaApiKey> ToReturn = new List<AletheiaApiKey>();
            while (dr.Read())
            {
                AletheiaApiKey thiskey = new AletheiaApiKey();

                if (dr.IsDBNull(0) == false)
                {
                    thiskey.Token = dr.GetGuid(0);
                }

                if (dr.IsDBNull(1) == false)
                {
                    thiskey.CreatedAtUtc = dr.GetDateTime(1);
                }

                ToReturn.Add(thiskey);
            }

            sqlcon.Close();
            return ToReturn.ToArray();
        }


        public async Task<bool> ApiKeyExistsAsync(Guid token)
        {
            string cmd = "select count(Token) from ApiKey where Token = '" + token.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            if (val > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public async Task<int> CountApiKeysAsync()
        {
            string cmd = "select Count(Token) from ApiKey";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        // API calls

        public async Task UploadApiCallAsync(AletheiaApiCall call)
        {
            int dir_int = 0;
            if (call.Direction == ApiCallDirection.Request)
            {
                dir_int = 0;
            }
            else if (call.Direction == ApiCallDirection.Push)
            {
                dir_int = 1;
            }

            //To use ID
            //I do this because previously this package generated the Id for you when uploading an ApiCall. It is possible systems that used this package in the past will not assign the Id themselves before passing it to this method. So if the ID is blank, generate one for us.
            if (call.Id == Guid.Empty)
            {
                call.Id = Guid.NewGuid();
            }

            TableInsertHelper tih = new TableInsertHelper("ApiCall");
            tih.AddColumnValuePair("Id", call.Id.ToString(), true);
            tih.AddColumnValuePair("CalledAtUtc", call.CalledAtUtc.ToString(), true);
            tih.AddColumnValuePair("ConsumedKey", call.ConsumedKey.ToString(), true);
            tih.AddColumnValuePair("Endpoint", Convert.ToInt32(call.Endpoint).ToString(), false);
            tih.AddColumnValuePair("Direction", dir_int.ToString());

            //Add the ResponseTime if it available
            if (call.ResponseTime.HasValue)
            {
                tih.AddColumnValuePair("ResponseTime", call.ResponseTime.Value.ToString(), false);
            }

            //Assemble command
            string cmd = tih.ToSqlCommand();

            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<int> CountKeysApiCallsDuringWindowAsync(Guid key_token, DateTime utc_begin, DateTime utc_end)
        {
            string cmd = "select count(Id) from ApiCall where ConsumedKey = '" + key_token +  "' and CalledAtUtc > '" + utc_begin.ToString() + "' and CalledAtUtc < '" + utc_end.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountKeysApiCallsDuringMonthAsync(Guid key_token, int year, int month)
        {
            string cmd = "select count(Id) from ApiCall where ConsumedKey = '" + key_token + "' and month(CalledAtUtc) = " + month.ToString() + " and year(CalledAtUtc) = " + year.ToString();
            int ToReturn = await CountSqlCommandAsync(cmd);
            return ToReturn;
        }

        public async Task<int> CountApiCallsAsync(Guid? by_key = null)
        {
            string cmd = "select Count(Id) from ApiCall";
            if (by_key.HasValue)
            {
                cmd = cmd + " where ConsumedKey = '" + by_key.Value.ToString() + "'";
            }
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<AletheiaApiCall[]> GetLatestApiCallsAsync(int top = 10, Guid? by_key = null)
        {
            //is a by key specified?
            string usebk = "";
            if (by_key.HasValue)
            {
                usebk = "where ConsumedKey = '" + by_key.Value.ToString() + "'";
            }

            string cmd = "select top " + top + " Id, CalledAtUtc, ConsumedKey, Endpoint, Direction, ResponseTime from ApiCall " +usebk + "order by CalledAtUtc desc";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<AletheiaApiCall> ToReturn = new List<AletheiaApiCall>();
            while (dr.Read())
            {
                AletheiaApiCall call = ExtractApiCallFromSqlDataReader(dr);
                ToReturn.Add(call);
            }

            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<AletheiaApiCall[]> GetLatestApiCallsAsync(AletheiaEndpoint endpoint, int top = 10)
        {
            if (top < 1)
            {
                throw new Exception("The 'top' parameter of the 'GetLatestApiCallsAsync' method must be greater than 0");
            }
            string cmd = "select top " + top.ToString() + " Id, CalledAtUtc, ConsumedKey, Endpoint, Direction, ResponseTime from ApiCall where Endpoint = " + Convert.ToInt32(endpoint).ToString() + " order by CalledAtUtc desc";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<AletheiaApiCall> ToReturn = new List<AletheiaApiCall>();
            while (dr.Read())
            {
                ToReturn.Add(ExtractApiCallFromSqlDataReader(dr));
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        private AletheiaApiCall ExtractApiCallFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            AletheiaApiCall ToReturn = new AletheiaApiCall();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {
                
            }

            //CalledAtUtc
            try
            {
                ToReturn.CalledAtUtc = dr.GetDateTime(dr.GetOrdinal(prefix + "CalledAtUtc"));
            }
            catch
            {

            }

            //Consumed Key
            try
            {
                ToReturn.ConsumedKey = dr.GetGuid(dr.GetOrdinal(prefix + "ConsumedKey"));
            }
            catch
            {

            }

            //Endpoint
            try
            {
                ToReturn.Endpoint = (AletheiaEndpoint)dr.GetByte(dr.GetOrdinal(prefix + "Endpoint"));
            }
            catch
            {
                
            }

            //Direction
            try
            {
                bool dirval = dr.GetBoolean(dr.GetOrdinal(prefix + "Direction"));
                if (dirval == false)
                {
                    ToReturn.Direction = ApiCallDirection.Request;
                }
                else
                {
                    ToReturn.Direction = ApiCallDirection.Push;
                }
            }
            catch
            {

            }
            
            //ResponseTime
            try
            {
                ToReturn.ResponseTime = dr.GetFloat(dr.GetOrdinal(prefix + "ResponseTime"));
            }
            catch
            {
                ToReturn.ResponseTime = null;
            }

            return ToReturn;
        }

        public async Task SetApiCallResponseTimeAsync(Guid call_id, float? response_time)
        {
            string cmd = "";
            if (response_time.HasValue)
            {
                cmd = "update ApiCall set ResponseTime = " + response_time.Value.ToString() + " where Id = '" + call_id.ToString() + "'";
            }
            else
            {
                cmd = "update ApiCall set ResponseTime = null where Id = '" + call_id.ToString() + "'";
            }
            
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        //Gets a list of all unique api call titles in the SQL DB
        public async Task<AletheiaEndpoint[]> GetUsedApiCallEndpointsAsync()
        {
            string cmd = "select distinct Endpoint from ApiCall";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<AletheiaEndpoint> ToReturn = new List<AletheiaEndpoint>();
            while (dr.Read())
            {
                AletheiaEndpoint ep = (AletheiaEndpoint)dr.GetByte(0);
                ToReturn.Add(ep);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<AletheiaEndpoint[]> GetUsedApiCallEndpointsAsync(DateTime on_utc_day)
        {
            string cmd = "select distinct Endpoint from ApiCall where YEAR(CalledAtUtc) = " + on_utc_day.Year.ToString() + " and MONTH(CalledAtUtc) = " + on_utc_day.Month.ToString() + " and DAY(CalledAtUtc) = " + on_utc_day.Day.ToString();
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<AletheiaEndpoint> ToReturn = new List<AletheiaEndpoint>();
            while (dr.Read())
            {
                AletheiaEndpoint ep = (AletheiaEndpoint)dr.GetByte(0);
                ToReturn.Add(ep);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        //Count the number of API calls for a particular endpoint by name
        public async Task<int> CountApiCallsAsync(AletheiaEndpoint endpoint)
        {
            string cmd = "select count(Endpoint) from ApiCall where Endpoint = " + Convert.ToInt32(endpoint).ToString();
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        public async Task<int> CountApiCallsAsync(DateTime on_utc_day, ApiCallDirection? filter_to_direction = null)
        {
            string where_clause = "";
            if (filter_to_direction.HasValue)
            {
                if (filter_to_direction.Value == ApiCallDirection.Push)
                {
                    where_clause = " and Direction = 1";
                }
                else if (filter_to_direction.Value == ApiCallDirection.Request)
                {
                    where_clause = " and Direction = 0";
                }
            }

            string day = on_utc_day.Day.ToString();
            string month = on_utc_day.Month.ToString();
            string year = on_utc_day.Year.ToString();
            string cmd = "select count(Endpoint) from ApiCall where YEAR(CalledAtUtc) = " + year + " and MONTH(CalledAtUtc) = " + month + " and DAY(CalledAtUtc) = " + day + where_clause;
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        public async Task<int> CountApiCallsAsync(DateTime on_utc_day, AletheiaEndpoint endpoint)
        {
            string day = on_utc_day.Day.ToString();
            string month = on_utc_day.Month.ToString();
            string year = on_utc_day.Year.ToString();
            string cmd = "select count(Endpoint) from ApiCall where YEAR(CalledAtUtc) = " + year + " and MONTH(CalledAtUtc) = " + month + " and DAY(CalledAtUtc) = " + day + " and Endpoint = " + Convert.ToInt32(endpoint).ToString();
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        #endregion

        #region "Fundamentals related tables"

        public async Task UploadFactContextAsync(FactContext fc)
        {
            string cmd = "insert into FactContext (Id, FromFiling, PeriodStart, PeriodEnd) values ('" + fc.Id.ToString() + "', ";
            //From filing?
            if (fc.FromFiling.HasValue)
            {
                cmd = cmd + "'" + fc.FromFiling.ToString() + "',";
            }
            else
            {
                cmd = cmd + "null, ";
            }
            //Dates
            cmd = cmd + "'" + fc.PeriodStart.ToString() + "', '" + fc.PeriodEnd.ToString() + "'";
            cmd = cmd + ")";
            
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task UploadFinancialFactAsync(FinancialFact ff)
        {
            string cmd = "insert into FinancialFact (Id, ParentContext, LabelId, Value) values ('" + ff.Id.ToString() + "', '" + ff.ParentContext.ToString() + "', " + Convert.ToInt32(ff.LabelId).ToString() + ", " + ff.Value.ToString() + ")";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<FinancialFact> GetFinancialFactAsync(Guid id)
        {
            string cmd = "select ParentContext, LabelId, Value from FinancialFact where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find Financial Fact with ID '" + id.ToString() + "'");
            }
            dr.Read();
            FinancialFact ToReturn = ExtractFinancialFactFromSqlDataReader(dr);
            sqlcon.Close();
            ToReturn.Id = id;
            return ToReturn;
        }

        public async Task<FactContext> GetFactContextAsync(Guid id)
        {
            string cmd = "select FromFiling, PeriodStart, PeriodEnd from FactContext where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find Fact Context with ID '" + id.ToString() + "'");
            }
            dr.Read();
            FactContext ToReturn = ExtractFactContextFromSqlDataReader(dr);
            sqlcon.Close();
            return ToReturn;
        }

        /// <summary>
        /// Uploads the Fact Contexts and Financial Facts contained in a processing result
        /// </summary>
        public async Task UploadFundamentalsProcessingResultAsync(FundamentalsProcessingResult fpr)
        {
            foreach (FactContext fc in fpr.FactContexts)
            {
                await UploadFactContextAsync(fc);
            }
            
            foreach (FinancialFact ff in fpr.FinancialFacts)
            {
                await UploadFinancialFactAsync(ff);
            }
        }

        public async Task DeleteChildFinancialFactsAsync(Guid parent_fact_context_id)
        {
            string cmd = "delete from FinancialFact where ParentContext = '" + parent_fact_context_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task DeleteFactContextsFromSecFilingAsync(Guid parent_sec_filing_id)
        {
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            string cmd_d = "delete from FactContext where FromFiling = '" + parent_sec_filing_id.ToString() + "'";
            SqlCommand sqlcmd_d = new SqlCommand(cmd_d, sqlcon);
            await sqlcmd_d.ExecuteNonQueryAsync();
            sqlcon.Close();      
        }

        public async Task DeleteFactContextsFromSecFilingAsync(string filing_url)
        {
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            string cmd_d = "delete fc from FactContext fc inner join SecFiling sf on fc.FromFiling = sf.Id where sf.FilingUrl = '" + filing_url + "'";
            SqlCommand sqlcmd_d = new SqlCommand(cmd_d, sqlcon);
            await sqlcmd_d.ExecuteNonQueryAsync();
            sqlcon.Close();   
        }

        public async Task DeleteFinancialFactsFromSecFilingAsync(Guid sec_filing_id)
        {
            string cmd = "delete ff from FinancialFact ff inner join FactContext fc on ff.ParentContext = fc.Id inner join SecFiling sf on fc.FromFiling = sf.Id where sf.Id = '" + sec_filing_id + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task DeleteFinancialFactsFromSecFilingAsync(string filing_url)
        {
            string cmd = "delete ff from FinancialFact ff inner join FactContext fc on ff.ParentContext = fc.Id inner join SecFiling sf on fc.FromFiling = sf.Id where sf.FilingUrl = '" + filing_url + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<int> CountFinancialFactsFromSecFilingAsync(Guid sec_filing_id)
        {
            string cmd = "select count(ff.Id) from FinancialFact ff inner join FactContext fc on ff.ParentContext = fc.Id inner join SecFiling sf on fc.FromFiling = sf.Id where sf.Id = '" + sec_filing_id.ToString() + "'";
            int ToReturn = await CountSqlCommandAsync(cmd);
            return ToReturn;
        }

        public async Task<int> CountFinancialFactsFromSecFilingAsync(string filing_url)
        {
            string cmd = "select count(ff.Id) from FinancialFact ff inner join FactContext fc on ff.ParentContext = fc.Id inner join SecFiling sf on fc.FromFiling = sf.Id where sf.FilingUrl = '" + filing_url + "'";
            int ToReturn = await CountSqlCommandAsync(cmd);
            return ToReturn;
        }

        public async Task<int> CountFactContextsFromSecFilingAsync(Guid sec_filing_id)
        {
            string cmd = "select count(FactContext.Id) from FactContext inner join SecFiling on FactContext.FromFiling = SecFiling.Id where SecFiling.Id = '" + sec_filing_id.ToString() + "'";
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        public async Task<int> CountFactContextsFromSecFilingAsync(string filing_url)
        {
            string cmd = "select count(FactContext.Id) from FactContext inner join SecFiling on FactContext.FromFiling = SecFiling.Id where SecFiling.FilingUrl = '" + filing_url + "'";
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        

        //////////// Abstract methods (the above are just used for CRUD, these are meant to provide value)

        public async Task<FactContext> FindLatestFactContextAsync(long issuer_cik, FilingType? period_type = null, DateTime? before = null)
        {
            //Construct the command
            //string cmd = "select top 1 FactContext.Id, FactContext.FromFiling, FactContext.PeriodStart, FactContext.PeriodEnd from FactContext inner join SecFiling on FactContext.FromFiling = SecFiling.Id where SecFiling.FilingType = " + Convert.ToInt32(period_type).ToString() + " and SecFiling.Issuer = " + issuer_cik.ToString() + " and FactContext.PeriodEnd < '" + before.ToString() + "' order by FactContext.PeriodEnd desc";
            string cmd = "select top 1 FactContext.Id, FactContext.FromFiling, FactContext.PeriodStart, FactContext.PeriodEnd from FactContext inner join SecFiling on FactContext.FromFiling = SecFiling.Id where SecFiling.Issuer = " + issuer_cik.ToString();

            //Period type filter
            if (period_type.HasValue)
            {
                cmd = cmd + " and SecFiling.FilingType = " + Convert.ToInt32(period_type).ToString();
            }

            //Before filter
            if (before.HasValue)
            {
                cmd = cmd + " and FactContext.PeriodEnd < '" + before.ToString() + "'";
            }

            //Attach the sort
            cmd = cmd + " order by FactContext.PeriodEnd desc";

            FactContext ToReturn = null;   
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return ToReturn; //It should be null at this point.
            }
            else
            {
                dr.Read();
                ToReturn = ExtractFactContextFromSqlDataReader(dr);
                sqlcon.Close();
                return ToReturn;
            }
        }

        private FactContext ExtractFactContextFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            FactContext ToReturn = new FactContext();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //From Filing
            try
            {
                ToReturn.FromFiling = dr.GetGuid(dr.GetOrdinal(prefix + "FromFiling"));
            }
            catch
            {

            }

            //Period Start
            try
            {
                ToReturn.PeriodStart = dr.GetDateTime(dr.GetOrdinal(prefix + "PeriodStart"));
            }
            catch
            {

            }

            //Period end
            try
            {
                ToReturn.PeriodEnd = dr.GetDateTime(dr.GetOrdinal(prefix + "PeriodEnd"));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<FinancialFact[]> GetFinancialFactsAsync(Guid from_fact_context_id)
        {
            string cmd = "select Id, LabelId, Value from FinancialFact where ParentContext = '" + from_fact_context_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<FinancialFact> ToReturn = new List<FinancialFact>();
            while (dr.Read())
            {
                ToReturn.Add(ExtractFinancialFactFromSqlDataReader(dr));
            }
            sqlcon.Close();

            //Since we did not pull down the ID's (to save to time and we dont have to since that is a parameter and part of the query itself!), plug in the iD
            foreach (FinancialFact ff in ToReturn)
            {
                ff.ParentContext = from_fact_context_id;
            }


            return ToReturn.ToArray();
        }

        private FinancialFact ExtractFinancialFactFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            FinancialFact ToReturn = new FinancialFact();

            //ID
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Parent context
            try
            {
                ToReturn.ParentContext = dr.GetGuid(dr.GetOrdinal(prefix + "ParentContext"));
            }
            catch
            {

            }

            //LabelId
            try
            {
                short label_id = dr.GetInt16(dr.GetOrdinal(prefix + "LabelId"));
                ToReturn.LabelId = (FactLabel)label_id;
            }
            catch
            {

            }

            //Value
            try
            {
                ToReturn.Value = dr.GetFloat(dr.GetOrdinal(prefix + "Value"));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<FinancialFact[]> GetFinancialFactTrendAsync(long company_cik, FactLabel fact, FilingType? period_type = null, DateTime? after = null, DateTime? before = null)
        {
            //Assemble the commnad
            List<string> cmd = new List<string>();
            cmd.Add("select");
            cmd.Add("FinancialFact.Id as FF_Id,");
            cmd.Add("FinancialFact.Value as FF_Value,");
            cmd.Add("FactContext.PeriodStart as FC_PeriodStart,");
            cmd.Add("FactContext.PeriodEnd as FC_PeriodEnd");
            cmd.Add("from FinancialFact");
            cmd.Add("inner join FactContext on FinancialFact.ParentContext = FactContext.Id");
            cmd.Add("inner join SecFiling on FactContext.FromFiling = SecFiling.Id");
            cmd.Add("where");
            cmd.Add("SecFiling.Issuer = " + company_cik.ToString());
            cmd.Add("and FinancialFact.LabelId = " + Convert.ToInt32(fact).ToString());
            if (period_type.HasValue)
            {
                cmd.Add("and SecFiling.FilingType = " + Convert.ToInt32(period_type.Value).ToString());
            }
            if (after.HasValue)
            {
                cmd.Add("and FactContext.PeriodEnd > '" + after.ToString() + "'");
            }
            if (before.HasValue)
            {
                cmd.Add("and FactContext.PeriodEnd < '" + before.ToString() + "'");
            }
            string cmd_str = StringArrayToString(cmd.ToArray());

            //Call
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd_str, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<FinancialFact> ToReturnFacts = new List<FinancialFact>();
            while (dr.Read())
            {
                FinancialFact ff = ExtractFinancialFactFromSqlDataReader(dr, "FF_");
                FactContext fc = ExtractFactContextFromSqlDataReader(dr, "FC_");
                ff.LabelId = fact; //plug in the label ID since we have it (we did not request that data in the query, so plug it in here)
                ff._ParentContext = fc;
                ToReturnFacts.Add(ff);
            }

            sqlcon.Close();

            return SortFinancialFactsFromOldestToNewest(ToReturnFacts.ToArray());            
        }

        public async Task<FinancialFact[]> DeduceQuarterlyFinancialFactTrendAsync(long company_cik, FactLabel fact, DateTime? after = null, DateTime? before = null)
        {
            //Get all facts (both annual and quarterly)
            FinancialFact[] facts = GetFinancialFactTrendAsync(company_cik, fact, null, after, before).Result; //Get all (10-K and 10-Q)

            //Put them all in a list
            List<FinancialFact> AllQs = new List<FinancialFact>();
            foreach (FinancialFact ff in facts)
            {
                TimeSpan ts = ff._ParentContext.PeriodEnd - ff._ParentContext.PeriodStart;
                int days = ts.Days;
                if (days > 100) //It is annual
                {
                    try
                    {
                        FinancialFact df = await DeduceQ4FinancialFactAsync(ff.Id);
                        AllQs.Add(df);
                    }
                    catch
                    {

                    }
                }
                else //If it is quarterly, just add it
                {
                    AllQs.Add(ff);
                }
            }

            return SortFinancialFactsFromOldestToNewest(AllQs.ToArray());
        }

        //This is used by the above two methods for getting Financial fact trends. Ran at the end to ensure they are in proper order from oldest to newest.
        private FinancialFact[] SortFinancialFactsFromOldestToNewest(FinancialFact[] facts)
        {
            //Error check - does every FinancialFact have a parent context (_ParentContext)?
            foreach (FinancialFact ff in facts)
            {
                if (ff._ParentContext == null)
                {
                    throw new Exception("Unable to sort financial facts: fact '" + ff.Id.ToString() + "' with value '" + ff.Value.ToString("#,##0") + "' does not have a parent context.");
                }
            }

            List<FinancialFact> ToPullFrom = new List<FinancialFact>();
            ToPullFrom.AddRange(facts);

            List<FinancialFact> ToReturn = new List<FinancialFact>();
            while (ToPullFrom.Count > 0)
            {
                FinancialFact winner = ToPullFrom[0]; //This should be the oldest
                foreach (FinancialFact ff in ToPullFrom)
                {
                    DateTime this_period_end = ff._ParentContext.PeriodEnd;
                    DateTime winner_period_end = winner._ParentContext.PeriodEnd;
                    if (this_period_end < winner_period_end)
                    {
                        winner = ff;
                    }
                }
                ToReturn.Add(winner);
                ToPullFrom.Remove(winner);
            }

            return ToReturn.ToArray();            
        }

        public async Task<FinancialFact> DeduceQ4FinancialFactAsync(Guid year_end_fact_id)
        {
            //Get the financial fact
            FinancialFact yeff = await GetFinancialFactAsync(year_end_fact_id);

            //Get the year end facts parent fact context (to get the start/end date)
            FactContext yefc = await GetFactContextAsync(yeff.ParentContext);

            //Get the SEC filing for this fact context to know what the issuer's CIK is
            SecFiling filing = await GetSecFilingByIdAsync(yefc.FromFiling.Value);

            //Throw an error if this is actually not for a full year
            int ye_days = (yefc.PeriodEnd - yefc.PeriodStart).Days;
            if (ye_days < 350 || ye_days > 380)
            {
                throw new Exception("Year end fact ID '" + year_end_fact_id.ToString() + "' was not for a full year (# of days in period was " + ye_days.ToString() + ") Please only provide a full year end fact ID to deduce Q4 results.");
            }

            //Determine the dates to ask for
            DateTime Q3_End = yefc.PeriodEnd.AddDays(-90);
            DateTime Q3_Start = Q3_End.AddDays(-90);
            DateTime Q2_End = Q3_End.AddDays(-90);
            DateTime Q2_Start = Q2_End.AddDays(-90);
            DateTime Q1_End = Q2_End.AddDays(-90);
            DateTime Q1_Start = Q1_End.AddDays(-90);

            // Console.WriteLine("Q3: " + Q3_Start.ToShortDateString() + " - " + Q3_End.ToShortDateString());
            // Console.WriteLine("Q2: " + Q2_Start.ToShortDateString() + " - " + Q2_End.ToShortDateString());
            // Console.WriteLine("Q1: " + Q1_Start.ToShortDateString() + " - " + Q1_End.ToShortDateString());
            
            //Get them
            FinancialFact Q3 = await TryFindFinancialFactAsync(filing.Issuer, yeff.LabelId, Q3_Start, Q3_End);
            FinancialFact Q2 = await TryFindFinancialFactAsync(filing.Issuer, yeff.LabelId, Q2_Start, Q2_End);
            FinancialFact Q1 = await TryFindFinancialFactAsync(filing.Issuer, yeff.LabelId, Q1_Start, Q1_End);

            // Console.WriteLine("Q3: " + JsonConvert.SerializeObject(Q3));
            // Console.WriteLine("Q2: " + JsonConvert.SerializeObject(Q2));
            // Console.WriteLine("Q1: " + JsonConvert.SerializeObject(Q1));

            //Throw an error if unable to retrieve any of the quarters
            if (Q3 == null)
            {
                throw new Exception("Unable to find Q3 value for fact '" + yeff.LabelId.ToString() + "' for company " + filing.Issuer.ToString() + " with start date " + Q3_Start.ToShortDateString() + " and end date " + Q3_End.ToShortDateString() + ".");
            }
            if (Q2 == null)
            {
                throw new Exception("Unable to find Q2 value for fact '" + yeff.LabelId.ToString() + "' for company " + filing.Issuer.ToString() + " with start date " + Q2_Start.ToShortDateString() + " and end date " + Q2_End.ToShortDateString() + ".");
            }
            if (Q1 == null)
            {
                throw new Exception("Unable to find Q1 value for fact '" + yeff.LabelId.ToString() + "' for company " + filing.Issuer.ToString() + " with start date " + Q1_Start.ToShortDateString() + " and end date " + Q1_End.ToShortDateString() + ".");
            }

            //Calculate
            float Calculated = yeff.Value - Q3.Value - Q2.Value - Q1.Value;

            //Assemble
            FinancialFact ToReturn = new FinancialFact();
            ToReturn.Value = Calculated;
            ToReturn.LabelId = yeff.LabelId;

            //Set the contextual data (start date, end date) for this new fact (this data is contained in the parent context)
            ToReturn._ParentContext = new FactContext();
            ToReturn._ParentContext.PeriodStart = Q3_End.AddDays(1); //The day after the end of Q3
            ToReturn._ParentContext.PeriodEnd = ToReturn._ParentContext.PeriodStart.AddDays(90); //90 days (3 months), 1 quarter.

            return ToReturn;
        }
        
        //This is used exclusively in the "DeduceQ4FinancialFactAsync" method above. Will return null if not found.
        public async Task<FinancialFact> TryFindFinancialFactAsync(long company_cik, FactLabel fact, DateTime approx_start, DateTime approx_end)
        {
            //Settings
            int window = 10; //# of day window that the period start and end can be within and it is still a 'match'

            //Dates in YYYY/MM/DD format
            string ps = approx_start.Year.ToString("0000") + "/" + approx_start.Month.ToString("00") + "/" + approx_start.Day.ToString("00");
            string pe = approx_end.Year.ToString("0000") + "/" + approx_end.Month.ToString("00") + "/" + approx_end.Day.ToString("00");

            //Assemble the command
            List<string> SearchCmdArr = new List<string>();
            SearchCmdArr.Add("select FinancialFact.Id, FinancialFact.ParentContext, FinancialFact.LabelId, FinancialFact.Value from FinancialFact");
            SearchCmdArr.Add("inner join FactContext on FinancialFact.ParentContext = FactContext.Id");
            SearchCmdArr.Add("inner join SecFiling on FactContext.FromFiling = SecFiling.Id");
            SearchCmdArr.Add("where SecFiling.Issuer = " + company_cik.ToString());
            SearchCmdArr.Add("and FinancialFact.LabelId = " + Convert.ToInt32(fact).ToString());
            SearchCmdArr.Add("and ABS(DATEDIFF(day, FactContext.PeriodEnd, '" + pe + "')) < " + window.ToString());
            SearchCmdArr.Add("and ABS(DATEDIFF(day, FactContext.PeriodStart, '" + ps + "')) < " + window.ToString());
            SearchCmdArr.Add("order by FactContext.PeriodEnd desc");

            //Assemble into one string
            string cmd = "";
            foreach (string s in SearchCmdArr)
            {
                cmd = cmd + s + " ";
            }
            cmd = cmd.Substring(0, cmd.Length - 1); //Remove the last space

            //Make the call
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows)
            {
                dr.Read();
                FinancialFact ToReturn = ExtractFinancialFactFromSqlDataReader(dr);
                sqlcon.Close();
                return ToReturn;
            }
            else
            {
                sqlcon.Close();
                return null;
            }
        }

        #endregion

        #region "Earnings Call Related Tables"

        //UPLOADS
        public async Task UploadCallCompanyAsync(CallCompany cc)
        {
            string cmd = "insert into CallCompany (Id, Name, TradingSymbol) values ('" + cc.Id.ToString() + "', '" + cc.Name + "', '" + cc.TradingSymbol + "')";
            await ExecuteNonQueryAsync(cmd);            
        }

        public async Task UploadEarningsCallAsync(EarningsCall ec)
        {
            TableInsertHelper tih = new TableInsertHelper("EarningsCall");
            tih.AddColumnValuePair("Id", ec.Id.ToString(), true);
            tih.AddColumnValuePair("ForCompany", ec.ForCompany.ToString(), true);
            tih.AddColumnValuePair("Url", ec.Url, true);
            tih.AddColumnValuePair("Title", ec.Title, true);
            tih.AddColumnValuePair("Period", Convert.ToInt32(ec.Period).ToString(), false);
            tih.AddColumnValuePair("Year", ec.Year.ToString(), false);
            tih.AddColumnValuePair("HeldAt", ec.HeldAt.ToShortDateString(), true);
            string cmd = tih.ToSqlCommand();
            await ExecuteNonQueryAsync(cmd);
        }

        public async Task UploadSpokenRemarkAsync(SpokenRemark sr)
        {
            //First insert into SQL
            TableInsertHelper tih = new TableInsertHelper("SpokenRemark");
            tih.AddColumnValuePair("Id", sr.Id.ToString(), true);
            tih.AddColumnValuePair("FromCall", sr.FromCall.ToString(), true);
            tih.AddColumnValuePair("SpokenBy", sr.SpokenBy.ToString(), true);
            tih.AddColumnValuePair("SequenceNumber", sr.SequenceNumber.ToString(), false);
            await ExecuteNonQueryAsync(tih.ToSqlCommand());

            //Now upload it to azure blob storage
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("spokenremarks");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference(sr.Id.ToString());
            await blb.UploadTextAsync(sr.Remark);
        }

        public async Task UploadCallParticipantAsync(Aletheia.Engine.EarningsCalls.CallParticipant cp)
        {
            TableInsertHelper tih = new TableInsertHelper("CallParticipant");
            tih.AddColumnValuePair("Id", cp.Id.ToString(), true);
            tih.AddColumnValuePair("Name", cp.Name, true);
            tih.AddColumnValuePair("Title", cp.Title, true);
            tih.AddColumnValuePair("IsExternal", Convert.ToInt32(cp.IsExternal).ToString(), false);
            await ExecuteNonQueryAsync(tih.ToSqlCommand());
        }

        public async Task UploadSpokenRemarkHighlightAsync(SpokenRemarkHighlight srh)
        {
            TableInsertHelper tih = new TableInsertHelper("SpokenRemarkHighlight");
            tih.AddColumnValuePair("Id", srh.Id.ToString(), true);
            tih.AddColumnValuePair("SubjectRemark", srh.SubjectRemark.ToString(), true);
            tih.AddColumnValuePair("BeginPosition", srh.BeginPosition.ToString(), false);
            tih.AddColumnValuePair("EndPosition", srh.EndPosition.ToString(), false);
            tih.AddColumnValuePair("Category", Convert.ToInt32(srh.Category).ToString(), false);
            tih.AddColumnValuePair("Rating", srh.Rating.ToString(), false);
            await ExecuteNonQueryAsync(tih.ToSqlCommand());
        }


        //CHECKS
        public async Task<Guid?> EarningsCallExistsAsync(string url)
        {
            string cmd = "select Id from EarningsCall where Url = '" + url + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            else
            {
                await dr.ReadAsync();
                Guid g = dr.GetGuid(0);
                sqlcon.Close();
                return g;
            }   
        }

        public async Task<Guid?> CallCompanyExistsAsync(string trading_symbol)
        {
            string cmd = "select Id from CallCompany where TradingSymbol = '" + trading_symbol + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            else
            {
                await dr.ReadAsync();
                Guid nid = dr.GetGuid(0);
                sqlcon.Close();
                return nid;
            }   
        }

        public async Task<Guid?> CallParticipantExistsAsync(Aletheia.Engine.EarningsCalls.CallParticipant cp)
        {
            string cmd = "select Id from CallParticipant where Name = '" + cp.Name + "' and Title = '" + cp.Title + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            else
            {
                await dr.ReadAsync();
                Guid id = dr.GetGuid(0);
                sqlcon.Close();
                return id;
            }
        }


        //Downloads
        public async Task<CallCompany> GetCallCompanyAsync(Guid id)
        {
            string cmd = "select Name, TradingSymbol from CallCompany where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find CallCompany with Id '" + id.ToString() + "'");
            }
            await dr.ReadAsync();
            CallCompany ToReturn = ExtractCallCompanyFromSqlDataReader(dr, "");
            ToReturn.Id = id; //plug in the ID.
            sqlcon.Close();
            return ToReturn;
        }

        private CallCompany ExtractCallCompanyFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            CallCompany ToReturn = new CallCompany();
            
            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Name
            try
            {
                ToReturn.Name = dr.GetString(dr.GetOrdinal(prefix + "Name"));
            }
            catch
            {
                ToReturn.Name = null;
            }

            //Trading Symbol
            try
            {
                ToReturn.TradingSymbol = dr.GetString(dr.GetOrdinal(prefix + "TradingSymbol"));
            }
            catch
            {
                ToReturn.TradingSymbol = null;
            }

            return ToReturn;
        }

        public async Task<EarningsCall> GetEarningsCallAsync(Guid id)
        {
            string cmd = "select ForCompany, Url, Title, Period, Year, HeldAt from EarningsCall where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("EarningsCall with Id '" + id.ToString() + "' does not exist.");
            }
            await dr.ReadAsync();
            EarningsCall ToReturn = ExtractEarningsCallFromSqlDataReader(dr);
            ToReturn.Id = id;
            sqlcon.Close();
            return ToReturn;
        }

        private EarningsCall ExtractEarningsCallFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            EarningsCall ToReturn = new EarningsCall();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //For company
            try
            {
                ToReturn.ForCompany = dr.GetGuid(dr.GetOrdinal(prefix + "ForCompany"));
            }
            catch
            {

            }

            //Url
            try
            {
                ToReturn.Url = dr.GetString(dr.GetOrdinal(prefix + "Url"));
            }
            catch
            {
                ToReturn.Url = null;
            }

            //Title
            try
            {
                ToReturn.Title = dr.GetString(dr.GetOrdinal(prefix + "Title"));
            }
            catch
            {
                ToReturn.Title = null;
            }

            //Period
            try
            {
                ToReturn.Period = (FiscalPeriod)dr.GetByte(dr.GetOrdinal(prefix + "Period"));
            }
            catch
            {

            }

            //Year
            try
            {
                ToReturn.Year = Convert.ToInt32(dr.GetInt16(dr.GetOrdinal(prefix + "Year")));
            }
            catch
            {

            }

            //HeldAt
            try
            {
                ToReturn.HeldAt = dr.GetDateTime(dr.GetOrdinal(prefix + "HeldAt"));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<SpokenRemark> GetSpokenRemarkAsync(Guid id)
        {
            string cmd = "select FromCall, SpokenBy, SequenceNumber from SpokenRemark";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SpokenRemark with Id '" + id.ToString() + "'");
            }
            await dr.ReadAsync();
            SpokenRemark ToReturn = ExtractSpokenRemarkFromSqlDataReader(dr);
            ToReturn.Id = id;
            sqlcon.Close();

            //Now get the remark from Azure blob storage
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("spokenremarks");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference(id.ToString());
            if (blb.Exists() == false)
            {
                throw new Exception("Unable to find remark blob with name '" + id.ToString() + "'");
            }
            string content = await blb.DownloadTextAsync();
            ToReturn.Remark = content;

            return ToReturn;
        }
        
        private SpokenRemark ExtractSpokenRemarkFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SpokenRemark ToReturn = new SpokenRemark();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //FromCall
            try
            {
                ToReturn.FromCall = dr.GetGuid(dr.GetOrdinal(prefix + "FromCall"));
            }
            catch
            {

            }

            //SpokenBy
            try
            {
                ToReturn.SpokenBy = dr.GetGuid(dr.GetOrdinal(prefix + "SpokenBy"));
            }
            catch
            {

            }

            //Sequence number
            try
            {
                ToReturn.SequenceNumber = Convert.ToInt32(dr.GetInt16(dr.GetOrdinal(prefix + "SequenceNumber")));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<SpokenRemarkHighlight> GetSpokenRemarkHighlightAsync(Guid id)
        {
            string cmd = "select SubjectRemark, BeginPosition, EndPosition, Category, Rating from SpokenRemarkHighlight where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SpokenRemarkHighlight with Id '" + id.ToString() + "'");
            }
            await dr.ReadAsync();
            SpokenRemarkHighlight ToReturn = ExtractSpokenRemarkHighlightFromSqlDataReader(dr);
            ToReturn.Id = id;
            sqlcon.Close();
            return ToReturn;
        }

        private SpokenRemarkHighlight ExtractSpokenRemarkHighlightFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SpokenRemarkHighlight ToReturn = new SpokenRemarkHighlight();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //SubjectRemark
            try
            {
                ToReturn.SubjectRemark = dr.GetGuid(dr.GetOrdinal(prefix + "SubjectRemark"));
            }
            catch
            {

            }

            //BeginPosition
            try
            {
                ToReturn.BeginPosition = Convert.ToInt32(dr.GetInt16(dr.GetOrdinal(prefix + "BeginPosition")));
            }
            catch
            {

            }

            //EndPosition
            try
            {
                ToReturn.EndPosition = Convert.ToInt32(dr.GetInt16(dr.GetOrdinal(prefix + "EndPosition")));
            }
            catch
            {

            }

            //Category
            try
            {
                ToReturn.Category = (HighlightCategory)dr.GetByte(dr.GetOrdinal(prefix + "Category"));
            }
            catch
            {
                
            }

            //Rating
            try
            {
                ToReturn.Rating = dr.GetFloat(dr.GetOrdinal(prefix + "Rating"));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<Aletheia.Engine.EarningsCalls.CallParticipant> GetCallParticipantAsync(Guid id)
        {
            string cmd = "select Name, Title, IsExternal from CallParticipant where Id = '" + id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find CallParticipant with Id '" + id.ToString() + "'");
            }
            await dr.ReadAsync();
            Aletheia.Engine.EarningsCalls.CallParticipant ToReturn = ExtractCallParticipantFromSqlDataReader(dr);
            ToReturn.Id = id;
            sqlcon.Close();
            return ToReturn;
        }

        private Aletheia.Engine.EarningsCalls.CallParticipant ExtractCallParticipantFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            Aletheia.Engine.EarningsCalls.CallParticipant ToReturn = new Aletheia.Engine.EarningsCalls.CallParticipant();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //Name
            try
            {
                ToReturn.Name = dr.GetString(dr.GetOrdinal(prefix + "Name"));
            }
            catch
            {

            }

            //Title
            try
            {
                ToReturn.Title = dr.GetString(dr.GetOrdinal(prefix + "Title"));
            }
            catch
            {

            }

            //IsExternal
            try
            {
                ToReturn.IsExternal = dr.GetBoolean(dr.GetOrdinal(prefix + "IsExternal"));
            }
            catch
            {

            }

            return ToReturn;
        }


        //Deletes
        public async Task DeleteSpokenRemarkHighlightsFromEarningsCallAsync(Guid earnings_call_id)
        {
            string cmd = "delete srh from SpokenRemarkHighlight srh inner join SpokenRemark on srh.SubjectRemark = SpokenRemark.Id inner join EarningsCall on SpokenRemark.FromCall = EarningsCall.Id where EarningsCall.Id = '" + earnings_call_id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd);
        }

        public async Task DeleteSpokenRemarksFromEarningsCallAsync(Guid earnings_call_id)
        {
            //Get a list of the blobs that have to be deleted from azure blob storage
            Guid[] ToDeleteBlobs = await GetSpokenRemarkIdsFromEarningsCallAsync(earnings_call_id);
            
            //Now delete from azure blob storage
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("spokenremarks");
            await cont.CreateIfNotExistsAsync();
            foreach (Guid g in ToDeleteBlobs)
            {
                CloudBlockBlob blb = cont.GetBlockBlobReference(g.ToString());
                if (blb.Exists())
                {
                    await blb.DeleteAsync();
                }
            }

            //Delete from SQL
            string cmd = "delete sr from SpokenRemark sr inner join EarningsCall on sr.FromCall = EarningsCall.Id  where EarningsCall.Id = '" + earnings_call_id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd);    
        }

        private async Task<Guid[]> GetSpokenRemarkIdsFromEarningsCallAsync(Guid earnings_call_id)
        {
            string cmd = "select Id from SpokenRemark where FromCall = '" + earnings_call_id.ToString() + "'";
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<Guid> ToReturn = new List<Guid>();
            while (dr.Read())
            {
                Guid g = dr.GetGuid(0);
                ToReturn.Add(g);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task DeleteEarningsCallAsync(Guid earnings_call_id)
        {
            string cmd = "delete from EarningsCall where Id = '" + earnings_call_id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd);
        }


        //Higher level
        public async Task<Guid[]> GetSpotlightSpokenRemarksAsync(Guid earnings_call_id, HighlightCategory? category, Guid? spoken_by, int top = 10)
        {
            //Assemble the command
            List<string> cmd = new List<string>();
            cmd.Add("select top " + top.ToString());
            cmd.Add("sum(Rating) as r,");
            cmd.Add("SpokenRemarkHighlight.SubjectRemark");
            cmd.Add("from SpokenRemarkHighlight");

            //Joins
            cmd.Add("inner join SpokenRemark on SpokenRemarkHighlight.SubjectRemark = SpokenRemark.Id");

            //Where statements
            cmd.Add("where SpokenRemark.FromCall = '" + earnings_call_id + "'");
            if (category.HasValue)
            {
                cmd.Add("and SpokenRemarkHighlight.Category = " + Convert.ToInt32(category).ToString());
            }
            if (spoken_by.HasValue)
            {
                cmd.Add("and SpokenRemark.SpokenBy = '" + spoken_by.Value.ToString() + "'");
            }

            //Sort
            cmd.Add("group by SpokenRemarkHighlight.SubjectRemark");
            cmd.Add("order by r desc");

            //Assemble into one string
            string cmdstr = "";
            foreach (string s in cmd)
            {
                cmdstr = cmdstr + s + Environment.NewLine;
            }
            cmdstr = cmdstr.Substring(0, cmdstr.Length - 1);

            //Make the call
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmdstr, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<Guid> ToReturn = new List<Guid>();
            while (dr.Read())
            {
                ToReturn.Add(dr.GetGuid(1));
            }
            sqlcon.Close();
            return ToReturn.ToArray();            
        }


        #endregion

        #region "DB Statistic methods"

        public async Task<int> CountSecEntitiesAsync(bool with_trading_symbol = false)
        {
            string cmd = "select count(Cik) from SecEntity";
            if (with_trading_symbol)
            {
                cmd = cmd + " where TradingSymbol is not null";
            }
            int ToReturn = await CountSqlCommandAsync(cmd); 
            return ToReturn;
        }

        public async Task<int> CountSecFilingsAsync()
        {
            int ToReturn = await CountSqlCommandAsync("select count(Id) from SecFiling");
            return ToReturn;
        }

        public async Task<int> CountSecurityTransactionHoldingsAsync()
        {
            int ToReturn = await CountSqlCommandAsync("select count(Id) from SecurityTransactionHolding");
            return ToReturn;
        }

        public async Task<int> CountHeldOfficerPositionsAsync()
        {
            int ToReturn = await CountSqlCommandAsync("select count(Id) from HeldOfficerPosition");
            return ToReturn;
        }

        public async Task<int> CountFactContextsAsync()
        {
            string cmd = "select count(Id) from FactContext";
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        public async Task<int> CountFinancialFactsAsync()
        {
            string cmd = "select count(Id) from FinancialFact";
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }
        
        public async Task<int> CountFinancialFactsAsync(long for_company)
        {
            string cmd = "select Count(FinancialFact.Id) from FinancialFact inner join FactContext on FinancialFact.ParentContext = FactContext.Id inner join SecFiling on FactContext.FromFiling = SecFiling.Id where SecFiling.Issuer = " + for_company.ToString();
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        public async Task<int> CountFinancialFactsAsync(string for_company_symbol)
        {
            string cmd = "select Count(FinancialFact.Id) from FinancialFact inner join FactContext on FinancialFact.ParentContext = FactContext.Id inner join SecFiling on FactContext.FromFiling = SecFiling.Id inner join SecEntity on SecFiling.Issuer = SecEntity.Cik where SecEntity.TradingSymbol = '" + for_company_symbol + "'";
            int val = await CountSqlCommandAsync(cmd);
            return val;
        }

        private async Task<int> CountSqlCommandAsync(string cmd)
        {
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }
        
        #endregion

        #region "SQL DB Performance"

        //Uses the System resource stats in Azure SQL servers
        //Found here: https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-db-resource-stats-azure-sql-database?view=azuresqldb-current
        //So the only reason these work is because this is a Microsoft based SQL server.

        public async Task<float> GetSqlDbCpuUtilizationPercentAsync()
        {
            string cmd = "select top 1 avg_cpu_percent from sys.dm_db_resource_stats order by end_time desc";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            float val = Convert.ToSingle(dr.GetDecimal(0));
            sqlcon.Close();
            return val;
        }

        public async Task<float> GetSqlDbMemoryUtilizationPercentAsync()
        {
            string cmd = "select top 1 avg_memory_usage_percent from sys.dm_db_resource_stats order by end_time desc";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            float val = Convert.ToSingle(dr.GetDecimal(0));
            sqlcon.Close();
            return val;
        }

        public async Task<float> GetSqlDbDataIoUtilizationPercentAsync()
        {
            string cmd = "select top 1 avg_data_io_percent from sys.dm_db_resource_stats order by end_time desc";
            SqlConnection sqlcon = GetSqlConnection();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            float val = Convert.ToSingle(dr.GetDecimal(0));
            sqlcon.Close();
            return val;
        }

        #endregion

        #endregion

        #region "Azure Blob Storage"

        #region "New Filing Triggering" 

        /// <summary>
        /// Provides the URL of the last seen SEC filing at the latest filing endpoint (https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent). Finds the URL stored in blob storage. Will return null if there is not one.
        /// </summary>
        public async Task<string> GetLastObservedFilingAsync()
        {
            string ToReturn = null;
            
            // BlobServiceClient bsc = new BlobServiceClient(CredentialPackage.AzureStorageConnectionString);
            // BlobContainerClient bcc = await bsc.CreateBlobContainerAsync("general");
            // await bcc.CreateIfNotExistsAsync();
            // BlobClient bc = bcc.GetBlobClient("LastObservedFiling");
            // if (bc.Exists())
            // {
            //     BlobDownloadInfo bdi = await bc.DownloadAsync();
            //     Stream s = bdi.Content;
            //     StreamReader sr = new StreamReader(s);
            //     string as_str = await sr.ReadToEndAsync();
            //     ToReturn = as_str;
            // }


            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            if (cont.Exists())
            {
                CloudBlockBlob blb = cont.GetBlockBlobReference("LastObservedFiling");
                if (blb.Exists())
                {
                    ToReturn=  await blb.DownloadTextAsync();
                }
            }
        
            return ToReturn;
        }

        public async Task SetLastObservedFilingAsync(string url)
        {
            // BlobServiceClient bsc = new BlobServiceClient(CredentialPackage.AzureStorageConnectionString);
            // BlobContainerClient bcc = bsc.GetBlobContainerClient("general");
            // await bcc.CreateIfNotExistsAsync();
            // BlobClient bc = bcc.GetBlobClient("LastObservedFiling");
            // MemoryStream ms = new MemoryStream();
            // StreamWriter sw = new StreamWriter(ms);
            // await sw.WriteLineAsync("Hello world!");
            // ms.Seek(0, SeekOrigin.Begin);
            // await bc.UploadAsync(ms, true);

            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("LastObservedFiling");
            await blb.UploadTextAsync(url);
        }

        public async Task<EdgarLatestFilingResult[]> GetNewFilingsAsync()
        {
            //Get the last seen url
            string last_seen_url = await GetLastObservedFilingAsync();

            //Setup
            List<EdgarLatestFilingResult> ToReturn = new List<EdgarLatestFilingResult>();
            EdgarLatestFilingsSearch lfs = await EdgarLatestFilingsSearch.SearchAsync();

            if (last_seen_url != null) //If ther is a last seen url, go through all of them up until the point we see the last seen one again.
            {
                foreach (EdgarLatestFilingResult esr in lfs.Results)
                {
                    if (esr.DocumentsUrl == last_seen_url)
                    {
                        break;
                    }
                    else
                    {
                        ToReturn.Add(esr);
                    }
                }
            }
            else //If there isnt a last seen url, just add them all.
            {
                ToReturn.AddRange(lfs.Results);
            }
            
            //Now before closing out, set the last observed filing
            await SetLastObservedFilingAsync(lfs.Results[0].DocumentsUrl);

            return ToReturn.ToArray();
        }

        public async Task CreateHttpPostTaskInQueueStorageAsync(string end_point, JObject to_post)
        {
            JObject GoingToPost = new JObject();
            GoingToPost["endpoint"] = end_point;
            GoingToPost["body"] = to_post;
            
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudQueueClient cqc = csa.CreateCloudQueueClient();
            CloudQueue cq = cqc.GetQueueReference("posttasks");
            await cq.CreateIfNotExistsAsync();
            await cq.AddMessageAsync(new CloudQueueMessage(JsonConvert.SerializeObject(GoingToPost)));
        }

        #endregion

        #region "API service shutoff"

        public async Task<ApiServiceShutoffSettings> DownloadShutoffSettingsAsync()
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("ServiceShutoff");
            bool exists = await blb.ExistsAsync();
            if (exists)
            {
                string content = await blb.DownloadTextAsync();
                try
                {
                    ApiServiceShutoffSettings ToReturn = JsonConvert.DeserializeObject<ApiServiceShutoffSettings>(content);
                    return ToReturn;
                }
                catch (Exception ex)
                {
                    throw new Exception("Unable to convert cloud shutoff content to object. Msg: " + ex.Message);
                }
            }
            else //If it doesn't exist, make a fresh one, upload the fresh one, and then return the fresh one
            {
                ApiServiceShutoffSettings ToReturn = new ApiServiceShutoffSettings();

                //Upload it before returning
                await blb.UploadTextAsync(JsonConvert.SerializeObject(ToReturn));

                return ToReturn;
            }
        }

        public async Task UploadShutoffSettingsAsync(ApiServiceShutoffSettings settings)
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("ServiceShutoff");
            try
            {
                await blb.UploadTextAsync(JsonConvert.SerializeObject(settings));
            }
            catch (Exception ex)
            {
                throw new Exception("Error while uploading Api Service shutoff settings. Msg: " + ex.Message);
            }
        }

        #endregion

        #region "The Motley fool earnings call transcripts"

        public async Task SetLastObservedTheMotleyFoolEarningsCallTranscriptUrlAsync(string url)
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("LastObservedTheMotleyFoolEarningsCallTranscript");
            await blb.UploadTextAsync(url);
        }

        public async Task<string> GetLastObservedTheMotleyFoolEarningsCallTranscriptUrlAsync()
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("LastObservedTheMotleyFoolEarningsCallTranscript");
            if (blb.Exists())
            {
                string content = await blb.DownloadTextAsync();
                return content;
            }
            else
            {
                return null;
            }
        }

        public async Task<TranscriptPreview[]> GetNewTheMotleyFoolEarningsCallTranscriptsAsync()
        {
            //Get the last observed
            string LastObservedTranscript = await GetLastObservedTheMotleyFoolEarningsCallTranscriptUrlAsync();

            //Get them
            bool HitObserved = false;
            TranscriptSource ts = new TranscriptSource();
            TranscriptPreview[] prevs = await ts.GetRecentTranscriptPreviewsNextPageAsync();
            List<TranscriptPreview> ToReturn = new List<TranscriptPreview>(); //For collecting the transcript previews
            foreach (TranscriptPreview tp in prevs)
            {
                if (HitObserved == false)
                {
                    if (tp.Url == LastObservedTranscript)
                    {
                        HitObserved = true;
                    }
                    else
                    {
                        ToReturn.Add(tp);
                    }
                }
            }

            //If the cart is full, take the most recent one and set it
            if (ToReturn.Count > 0)
            {
                await SetLastObservedTheMotleyFoolEarningsCallTranscriptUrlAsync(prevs[0].Url);
            }

            return ToReturn.ToArray();
        }

        #endregion        

        #endregion

        #region "Graph API"

        public async Task UploadMicrosoftGraphHelperStateAsync(MicrosoftGraphHelper mgh)
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("MicrosoftGraphHelper");
            await blb.UploadTextAsync(JsonConvert.SerializeObject(mgh));
        }

        public async Task<MicrosoftGraphHelper> RetrieveMicrosoftGraphHelperStateAsync()
        {
            CloudStorageAccount csa;
            CloudStorageAccount.TryParse(CredentialPackage.AzureStorageConnectionString, out csa);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();
            CloudBlobContainer cont = cbc.GetContainerReference("general");
            await cont.CreateIfNotExistsAsync();
            CloudBlockBlob blb = cont.GetBlockBlobReference("MicrosoftGraphHelper");
            if (blb.Exists() == false)
            {
                throw new Exception("A MicrosoftGraphHelper state does not exist in blob storage.");
            }
            string content = await blb.DownloadTextAsync();
            MicrosoftGraphHelper mgh = JsonConvert.DeserializeObject<MicrosoftGraphHelper>(content);
            return mgh;
        }

        #endregion

        #region "Utility functions"

        public async Task ExecuteNonQueryAsync(string cmd)
        {
            await GovernSqlCpuAsync();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        private SqlConnection GetSqlConnection()
        {
            SqlConnection sqlcon = new SqlConnection(CredentialPackage.SqlConnectionString);
            return sqlcon;
        }

        private class CreateTableHelper
        {
            private string TableName;
            private List<string> ColumnNameTypePairs;

            public CreateTableHelper(string table_name)
            {
                TableName = table_name;
                ColumnNameTypePairs = new List<string>();
            }

            public void AddColumnNameTypePair(string value)
            {
                ColumnNameTypePairs.Add(value);
            }

            public string ToCreateCommand()
            {
                string cmd = "create table " + TableName + " (";
                foreach (string s in ColumnNameTypePairs)
                {
                    cmd = cmd + s + ",";
                }
                cmd = cmd.Substring(0, cmd.Length-1);
                cmd = cmd + ")";
                return cmd;
            }

        }

        private class TableInsertHelper
        {
            private string TableName;
            private List<KeyValuePair<string, string>> ColumnValuePairs;

            public TableInsertHelper(string table_name)
            {
                TableName = table_name;
                ColumnValuePairs = new List<KeyValuePair<string, string>>();
            }

            public void AddColumnValuePair(string column_name, string value, bool add_quotes = false)
            {
                string value_to_add = value;
                if (add_quotes)
                {
                    value_to_add = "'" + value_to_add + "'";
                }
                ColumnValuePairs.Add(new KeyValuePair<string, string>(column_name, value_to_add));
            }

            public string ToSqlCommand()
            {
                //Get the columns piece
                string piece_cols = "";
                string piece_vals = "";
                foreach (KeyValuePair<string, string> kvp in ColumnValuePairs)
                {
                    piece_cols = piece_cols + kvp.Key + ",";
                    piece_vals = piece_vals + kvp.Value + ",";
                }
                piece_cols = piece_cols.Substring(0, piece_cols.Length - 1);
                piece_vals = piece_vals.Substring(0, piece_vals.Length - 1);
                piece_cols = "(" + piece_cols + ")";
                piece_vals = "(" + piece_vals + ")";

                string cmd = "insert into " + TableName + " " + piece_cols + " values " + piece_vals;
                return cmd;
            }
        }

        private string StringArrayToString(string[] lines)
        {
            string TR = "";
            foreach (string s in lines)
            {
                TR = TR + s + Environment.NewLine;
            }
            TR = TR.Substring(0, TR.Length-1);
            return TR;
        }

        #endregion

        #region "SQL Performance Governor"

        public float? SqlCpuGovernor {get; set;} //i.e. 40% is 0.4, 90% is 0.9, etc. If this is null, it means there is NOT a governor.
        public TimeSpan SqlCpuGovernorCheckDelay {get; set;}

        public event GovernorApplied SqlCpuGovernorApplied;

        public async Task GovernSqlCpuAsync()
        {
            if (SqlCpuGovernor.HasValue)
            {
                //Set vars that will be used
                bool ContinueOn = false;
                float ReadCpuUsage = float.MaxValue;

                while (ContinueOn == false)
                {
                    ReadCpuUsage = await GetSqlDbCpuUtilizationPercentAsync();
                    if (ReadCpuUsage < (SqlCpuGovernor.Value * 100f))
                    {
                        ContinueOn = true;
                    }
                    else
                    {
                        //trigger the event
                        if (SqlCpuGovernorApplied != null)
                        {
                            SqlCpuGovernorApplied.Invoke(SqlCpuGovernor.Value, ReadCpuUsage / 100f); //Divide by 100 because the SQL returns the percentage as 40 if it is 40%, not 0.4.
                        }

                        await Task.Delay(SqlCpuGovernorCheckDelay);
                    }
                }
            }
        }

        #endregion

    }
}