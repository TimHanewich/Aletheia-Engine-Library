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
using Aletheia.Cloud.User;

namespace Aletheia.Cloud
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
        }

        #region "SQL"

        public async Task InitializeSqlStorageAsync()
        {
            //Open the SQL connection
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();

            //Get a list of all the tables in the DB right now
            SqlCommand sqlcmdtns = new SqlCommand("select TABLE_NAME from information_schema.tables", sqlcon);
            SqlDataReader dr = await sqlcmdtns.ExecuteReaderAsync();
            List<string> ExistingTableNames = new List<string>();
            while (dr.Read())
            {
                if (dr.IsDBNull(0) == false)
                {
                    ExistingTableNames.Add(dr.GetString(0));
                }
            }
            dr.Close();

            #region "Insider Trading"
            
            //SEC Entity
            if (ExistingTableNames.Contains("SecEntity") == false)
            {
                string cmd = "create table SecEntity (Cik bigint primary key not null, Name varchar(255), TradingSymbol varchar(16))";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //SEC Filing
            if (ExistingTableNames.Contains("SecFiling") == false)
            {
                CreateTableHelper cth = new CreateTableHelper("SecFiling");
                cth.AddColumnNameTypePair("Id uniqueidentifier primary key not null");
                cth.AddColumnNameTypePair("FilingUrl varchar(255)");
                cth.AddColumnNameTypePair("AccessionP1 bigint");
                cth.AddColumnNameTypePair("AccessionP2 tinyint");
                cth.AddColumnNameTypePair("AccessionP3 int");
                cth.AddColumnNameTypePair("FilingType tinyint");
                cth.AddColumnNameTypePair("ReportedOn datetime");
                cth.AddColumnNameTypePair("Issuer bigint");
                cth.AddColumnNameTypePair("Owner bigint");
                string cmd = cth.ToCreateCommand();
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //Security Transaction Holding
            if (ExistingTableNames.Contains("SecurityTransactionHolding") == false)
            {
                CreateTableHelper cth = new CreateTableHelper("SecurityTransactionHolding");
                cth.AddColumnNameTypePair("Id uniqueidentifier primary key not null");
                cth.AddColumnNameTypePair("FromFiling uniqueidentifier");
                cth.AddColumnNameTypePair("EntryType bit");
                cth.AddColumnNameTypePair("AcquiredDisposed bit");
                cth.AddColumnNameTypePair("Quantity real");
                cth.AddColumnNameTypePair("PricePerSecurity float");
                cth.AddColumnNameTypePair("TransactionDate datetime");
                cth.AddColumnNameTypePair("TransactionCode tinyint");
                cth.AddColumnNameTypePair("QuantityOwnedFollowingTransaction real");
                cth.AddColumnNameTypePair("DirectIndirect bit");
                cth.AddColumnNameTypePair("SecurityTitle varchar(255)");
                cth.AddColumnNameTypePair("SecurityType bit");
                cth.AddColumnNameTypePair("ConversionOrExcercisePrice real");
                cth.AddColumnNameTypePair("ExcercisableDate datetime");
                cth.AddColumnNameTypePair("ExpirationDate datetime");
                cth.AddColumnNameTypePair("UnderlyingSecurityTitle varchar(255)");
                cth.AddColumnNameTypePair("UnderlyingSecurityQuantity real");
                SqlCommand sqlcmd = new SqlCommand(cth.ToCreateCommand(), sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //Held officer position
            if (ExistingTableNames.Contains("HeldOfficerPosition") == false)
            {
                CreateTableHelper cth = new CreateTableHelper("HeldOfficerPosition");
                cth.AddColumnNameTypePair("Id uniqueidentifier primary key not null");
                cth.AddColumnNameTypePair("Officer bigint");
                cth.AddColumnNameTypePair("Company bigint");
                cth.AddColumnNameTypePair("PositionTitle varchar(255)");
                cth.AddColumnNameTypePair("ObservedOn uniqueidentifier");
                SqlCommand sqlcmd = new SqlCommand(cth.ToCreateCommand(), sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            #endregion

            #region "Webhook subscription tables"

            //New Filings
            if (ExistingTableNames.Contains("WHSubs_NewFilings") == false)
            {
                string cmd = "create table WHSubs_NewFilings (Id uniqueidentifier primary key not null, Endpoint varchar(4000), AddedAtUtc datetime, RegisteredToKey uniqueidentifier)";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            #endregion

            #region "User-Related tables"

            //UserAccount
            if (ExistingTableNames.Contains("UserAccount") == false)
            {
                string cmd = "create table UserAccount (Id uniqueidentifier primary key not null, Username varchar(15), Password varchar(64), Email varchar(255), CreatedAtUtc datetime)";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //ApiKey
            if (ExistingTableNames.Contains("ApiKey") == false)
            {
                string cmd = "create table ApiKey (Token uniqueidentifier primary key not null, RegisteredTo uniqueidentifier, CreatedAtUtc datetime)";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //ApiCall
            if (ExistingTableNames.Contains("ApiCall") == false)
            {
                string cmd = "create table ApiCall (Id uniqueidentifier primary key not null, CalledAtUtc datetime, ConsumedKey uniqueidentifier, Endpoint varchar(255), Direction bit)";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            #endregion

            sqlcon.Close();
        }

        #region "Insider Trading"

        #region "SecFiling"

        public async Task<Guid?> FindSecFilingAsync(string accession_number)
        {
            List<string> Splitter = new List<string>();
            Splitter.Add("-");
            string[] parts = accession_number.Split(Splitter.ToArray(), StringSplitOptions.None);
            Guid? ToReturn = await FindSecFilingAsync(Convert.ToInt64(parts[0]), Convert.ToInt32(parts[1]), Convert.ToInt32(parts[2]));
            return ToReturn;
        }
        
        //Returns a GUID ID of the sec filing if it was found, null if not found (doesn't exist)
        public async Task<Guid?> FindSecFilingAsync(long accessionP1, int accessionP2, int accessionP3)
        {
            string cmd = "select Id from SecFiling where AccessionP1 = " + accessionP1.ToString() + " and AccessionP2 = " + accessionP2.ToString() + " and AccessionP3 = " + accessionP3.ToString();
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
            tih.AddColumnValuePair("Owner", filing.Owner.ToString(), true);
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<SecFiling> GetSecFilingByIdAsync(Guid id)
        {
            string cmd = "select Id, FilingUrl, AccessionP1, AccessionP2, AccessionP3, FilingType, ReportedOn, Issuer, Owner from SecFiling where Id = '" + id.ToString() + "'";
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
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();           
        }

        public async Task<SecEntity[]> SearchSecEntitiesAsync(string term, int top = 20)
        {
            string cmd = "select top " + top.ToString() + " Cik, Name, TradingSymbol from SecEntity where Cik like '%" + term + "%' or Name like '%" + term + "%' or TradingSymbol like '%" + term + "%'";
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

        //Gets all of the people (owners) who have securities/at one point did have securities in a company (looks at Filings that attached the two together)
        public async Task<SecEntity[]> GetAffiliatedOwnersAsync(long company)
        {
            string cmd = "select distinct Person.Cik, Person.Name, Person.TradingSymbol from SecEntity as Company inner join SecFiling as Filing on Company.Cik = Filing.Issuer inner join SecEntity as Person on Filing.Owner = Person.Cik where Company.Cik = " + company.ToString();
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
                    if (sth.AcquiredDisposed == AcquiredDisposed.Acquired)
                    {
                        tih.AddColumnValuePair("AcquiredDisposed", "0");
                    }
                    else if (sth.AcquiredDisposed == AcquiredDisposed.Disposed)
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
                if (sth.ConversionOrExcercisePrice.HasValue)
                {
                    tih.AddColumnValuePair("ConversionOrExcercisePrice", sth.ConversionOrExcercisePrice.ToString());
                }
                
                //ExcercisableDate
                if (sth.ExcercisableDate.HasValue)
                {
                    tih.AddColumnValuePair("ExcercisableDate", sth.ExcercisableDate.ToString(), true);
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
                    ToReturn.AcquiredDisposed = AcquiredDisposed.Disposed;
                }
                else
                {
                    ToReturn.AcquiredDisposed = AcquiredDisposed.Acquired;
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
                ToReturn.ConversionOrExcercisePrice = dr.GetFloat(dr.GetOrdinal(prefix + "ConversionOrExcercisePrice"));
            }
            catch
            {

            }

            //ExcercisbleDate
            try
            {
                ToReturn.ExcercisableDate = dr.GetDateTime(dr.GetOrdinal(prefix + "ExcercisableDate"));
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

        #endregion

        #region "HeldOfficerPosition"

        public async Task<Guid?> FindHeldOfficerPositionAsync(long company, long officer, string position_title)
        {
            string cmd = "select Id from HeldOfficerPosition where Company = " + company.ToString() + " and Officer = " + officer.ToString() + " and PositionTitle = '" + position_title + "'";
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
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(tih.ToSqlCommand(), sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        #endregion

        #endregion

        #region "Webhook subscription tables"

        public async Task<Guid> AddNewFilingsWebhookSubscriptionAsync(WebhookSubscription sub)
        {
            Guid ToReturn = Guid.NewGuid();
            string cmd = "insert into WHSubs_NewFilings (Id, Endpoint, AddedAtUtc, RegisteredToKey) values ('" + ToReturn.ToString() + "', '" + sub.Endpoint + "', '" + sub.AddedAtUtc.ToString() + "', '" + sub.RegisteredToKey + "')";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<string[]> GetNewFilingsWebhookSubscriptionEndpointsAsync()
        {
            string cmd = "select distinct Endpoint from WHSubs_NewFilings";
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

        public async Task<WebhookSubscription[]> GetNewFilingsWebhookSubscriptionsAsync()
        {
            string cmd = "select Endpoint, AddedAtUtc, RegisteredToKey from WHSubs_NewFilings";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<WebhookSubscription> ToReturn = new List<WebhookSubscription>();
            while (dr.Read())
            {
                WebhookSubscription sub = new WebhookSubscription();
                
                if (dr.IsDBNull(0) == false)
                {
                    sub.Endpoint = dr.GetString(0);
                }

                if (dr.IsDBNull(1) == false)
                {
                    sub.AddedAtUtc = dr.GetDateTime(1);
                }

                if (dr.IsDBNull(2) == false)
                {
                    sub.RegisteredToKey = dr.GetGuid(2);
                }

                ToReturn.Add(sub);
            }

            sqlcon.Close();

            return ToReturn.ToArray();        
        }

        //Returns true if one was deleted, false if not.
        public async Task<bool> UnsubscribeFromNewFilingsWebhookByEndpointAsync(string endpoint)
        {
            string cmd = "delete from WHSubs_NewFilings where Endpoint = '" + endpoint + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            int affectedcount = await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            if (affectedcount > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        //Returns true if one was deleted, false if not.
        public async Task<bool> UnsubscribeFromNewFilingsWebhookByIdAsync(Guid id)
        {
            string cmd = "delete from WHSubs_NewFilings where Id = '" + id.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            int affectedcount = await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            if (affectedcount > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion

        #region "User-related tables"

        //USER ACCOUNTS

        public async Task<bool> UserAccountWithUsernameExistsAsync(string username)
        {
            string cmd = "select count(Id) from UserAccount where Username = '" + username + "'";
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

        public async Task<Guid> UploadUserAccountAsync(AletheiaUserAccount account)
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

            Guid ToReturn = Guid.NewGuid();
            string cmd = "insert into UserAccount (Id, Username, Password, Email, CreatedAtUtc) values ('" + ToReturn.ToString() + "', '" + account.Username + "', '" + account.Password + "', '" + account.Email + "', '" + account.CreatedAtUtc.ToString() + "')";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<AletheiaUserAccount> GetUserAccountByUsernameAsync(string username)
        {
            string cmd = "select Username, Password, Email, CreatedAtUtc from UserAccount where Username = '" + username + "'";
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
            AletheiaUserAccount ToReturn = new AletheiaUserAccount();
            dr.Read();

            if (dr.IsDBNull(0) == false)
            {
                ToReturn.Username = dr.GetString(0);
            }

            if (dr.IsDBNull(1) == false)
            {
                ToReturn.Password = dr.GetString(1);
            }

            if (dr.IsDBNull(2) == false)
            {
                ToReturn.Email = dr.GetString(2);
            }

            if (dr.IsDBNull(3) == false)
            {
                ToReturn.CreatedAtUtc = dr.GetDateTime(3);
            }

            sqlcon.Close();
            return ToReturn;
        }

        public async Task<AletheiaUserAccount> GetUserAccountByIdAsync(Guid id)
        {
            string cmd = "select Username, Password, Email, CreatedAtUtc from UserAccount where Id = '" + id.ToString() + "'";
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
            AletheiaUserAccount ToReturn = new AletheiaUserAccount();
            dr.Read();

            if (dr.IsDBNull(0) == false)
            {
                ToReturn.Username = dr.GetString(0);
            }

            if (dr.IsDBNull(1) == false)
            {
                ToReturn.Password = dr.GetString(1);
            }

            if (dr.IsDBNull(2) == false)
            {
                ToReturn.Email = dr.GetString(2);
            }

            if (dr.IsDBNull(3) == false)
            {
                ToReturn.CreatedAtUtc = dr.GetDateTime(3);
            }

            sqlcon.Close();
            return ToReturn;
        }

        public async Task<Guid> GetUserIdByUsernameAsync(string username)
        {
            string cmd = "select Id from UserAccount where Username = '" + username + "'";
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
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
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

            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }   

        public async Task<AletheiaApiKey[]> GetUsersApiKeysAsync(Guid user_id)
        {
            string cmd = "select Token, CreatedAtUtc from ApiKey where RegisteredTo = '" + user_id + "'";
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

        public async Task<Guid> UploadApiCallAsync(AletheiaApiCall call, Guid? consumed_key)
        {
            Guid ToReturn = Guid.NewGuid();

            int dir_int = 0;
            if (call.Direction == ApiCallDirection.Request)
            {
                dir_int = 0;
            }
            else if (call.Direction == ApiCallDirection.Push)
            {
                dir_int = 1;
            }

            string cmd = "";
            if (consumed_key.HasValue)
            {
                cmd = "insert into ApiCall (Id, CalledAtUtc, ConsumedKey, Endpoint, Direction) values ('" + ToReturn.ToString() + "', '" + call.CalledAtUtc.ToString() + "', '" + consumed_key.Value.ToString() + "', '" + call.Endpoint + "', " + dir_int.ToString() + ")";
            }
            else
            {
                cmd = "insert into ApiCall (Id, CalledAtUtc, Endpoint, Direction) values ('" + ToReturn.ToString() + "', '" + call.CalledAtUtc.ToString() + "', '" + call.Endpoint + "', " + dir_int.ToString() + ")";
            }

            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<int> CountKeysApiCallsDuringWindowAsync(Guid key_token, DateTime utc_begin, DateTime utc_end)
        {
            string cmd = "select count(Id) from ApiCall where ConsumedKey = '" + key_token +  "' and CalledAtUtc > '" + utc_begin.ToString() + "' and CalledAtUtc < '" + utc_end.ToString() + "'";
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
            DateTime Begin = new DateTime(year, month, 1);
            DateTime End = new DateTime(year, month, DateTime.DaysInMonth(year, month));
            int count = await CountKeysApiCallsDuringWindowAsync(key_token, Begin, End);
            return count;
        }

        #endregion

        #region "DB Statistic methods"

        public async Task<int> CountSecEntitiesAsync()
        {
            int ToReturn = await CountSqlCommandAsync("select count(Cik) from SecEntity");
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

        private async Task<int> CountSqlCommandAsync(string cmd)
        {
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
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            float val = Convert.ToSingle(dr.GetDecimal(0));
            sqlcon.Close();
            return val;
        }

        #endregion

        #endregion

        #region "New Filing Triggering (azure blob storage)" 

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

        #region "Utility functions"

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

        #endregion



    }
}