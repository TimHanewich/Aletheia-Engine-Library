using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Aletheia.Cloud
{
    public class AletheiaCloudClient
    {
        private string SqlConnectionString;

        public AletheiaCloudClient(string sql_connection_string)
        {
            SqlConnectionString = sql_connection_string;
        }

        public async Task InitializeStorageAsync()
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

            //Person
            if (ExistingTableNames.Contains("Person") == false)
            {
                string cmd = "create table Person (Cik char(10) not null primary key, FullName varchar(50))";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //SecurityTransaction
            if (ExistingTableNames.Contains("SecurityTransaction") == false)
            {
                string cmd = "create table SecurityTransaction (Id uniqueidentifier not null primary key, OwnedBy char(10), SecurityId uniqueidentifier, SecAccessionNumber char(20), AcquiredDisposed bit, Quantity real, TransactionDate datetime, TransactionCode tinyint, QuantityOwnedFollowingTransaction real, DirectIndirect bit)";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //Security
            if (ExistingTableNames.Contains("Security") == false)
            {
                string cmd = "create table Security (Id uniqueidentifier not null primary key, CompanyCik char(10), Title varchar(255), SecurityType bit, ConversionOrExcercisePrice real, ExcercisableDate datetime, ExpirationDate datetime, UnderlyingSecurityTitle varchar(255))";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            //Company
            if (ExistingTableNames.Contains("Company") == false)
            {
                string cmd = "create table Company (Cik char(10) not null primary key, TradingSymbol varchar(16), Name varchar(64))";
                SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
                await sqlcmd.ExecuteNonQueryAsync();
            }

            sqlcon.Close();
        }

        #region "Shallow (basic, single) uploading and downloading"

        public async Task UploadPersonAsync(Person p)
        {
            string use_name = p.FullName.Replace("'", "''"); //Make the apostraphees double (escape character)
            string cmd = "insert into Person (Cik,FullName) values ('" + p.CIK + "','" + use_name + "')";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        public async Task<Guid> UploadSecurityTransactionAsync(SecurityTransaction transaction)
        {
            Guid ToReturn = Guid.NewGuid();
            List<KeyValuePair<string, string>> ColumnValuePairs = new List<KeyValuePair<string, string>>();
            ColumnValuePairs.Add(new KeyValuePair<string, string>("Id", "'" + ToReturn.ToString() + "'"));
            //Skip OwnedBy (this is a refernece to another entity)
            //Skip SecurityId (this is a reference to another entity)
            ColumnValuePairs.Add(new KeyValuePair<string, string>("SecAccessionNumber", "'" + transaction.SecAccessionNumber + "'"));
            ColumnValuePairs.Add(new KeyValuePair<string, string>("QuantityOwnedFollowingTransaction", transaction.QuantityOwnedFollowingTransaction.ToString()));
            ColumnValuePairs.Add(new KeyValuePair<string, string>("DirectIndirect", Convert.ToInt32(transaction.DirectIndirect).ToString()));

            //Transaction Related - AcquiredDisposed
            if (transaction.AcquiredDisposed.HasValue)
            {
                ColumnValuePairs.Add(new KeyValuePair<string, string>("AcquiredDisposed", Convert.ToInt32(transaction.AcquiredDisposed.Value).ToString()));
            }

            //Transaction Related - Quantity
            if (transaction.Quantity.HasValue)
            {
                ColumnValuePairs.Add(new KeyValuePair<string, string>("Quantity", transaction.Quantity.Value.ToString()));
            }

            //Transaction Related - TransactionDate
            if (transaction.TransactionDate.HasValue)
            {
                string as_str = "'" + transaction.TransactionDate.Value.Year.ToString("0000") + "-" + transaction.TransactionDate.Value.Month.ToString("00") + "-" + transaction.TransactionDate.Value.Day.ToString("00") + " 00:00:00'";
                Console.WriteLine(as_str);
                ColumnValuePairs.Add(new KeyValuePair<string, string>("TransactionDate", as_str));
            }

            //Transaction Related - TransactionCode
            if (transaction.TransactionCode.HasValue)
            {
                ColumnValuePairs.Add(new KeyValuePair<string, string>("TransactionCode", Convert.ToInt32(transaction.TransactionCode.Value).ToString()));
            }

            //Prepare the values
            string part_columnnames = "";
            string part_values = "";
            foreach (KeyValuePair<string, string> kvp in ColumnValuePairs)
            {
                part_columnnames = part_columnnames + kvp.Key + ",";
                part_values = part_values + kvp.Value + ",";
            }
            part_columnnames = part_columnnames.Substring(0, part_columnnames.Length-1);
            part_values = part_values.Substring(0, part_values.Length-1);
            
            //Prepare  the command
            string cmd = "insert into SecurityTransaction (" + part_columnnames + ") values (" + part_values + ")";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();

            sqlcon.Close();
            return ToReturn;
        }

        public async Task<Guid> UploadSecurityAsync(Security security)
        {
            Guid ToReturn = Guid.NewGuid();

            List<KeyValuePair<string, string>> ColumnValuePairs = new List<KeyValuePair<string, string>>();
            ColumnValuePairs.Add(new KeyValuePair<string, string>("Id", "'" + ToReturn.ToString() + "'"));
            ColumnValuePairs.Add(new KeyValuePair<string, string>("CompanyCik", "'" + security.Company.CIK + "'"));
            ColumnValuePairs.Add(new KeyValuePair<string, string>("Title", "'" + security.Title + "'"));
            ColumnValuePairs.Add(new KeyValuePair<string, string>("SecurityType", Convert.ToInt32(security.SecurityType).ToString()));

            if (security.ConversionOrExcercisePrice.HasValue)
            {
                ColumnValuePairs.Add(new KeyValuePair<string, string>("ConversionOrExcercisePrice", security.ConversionOrExcercisePrice.Value.ToString()));
            }

            if (security.ExcercisableDate.HasValue)
            {
                string as_date = security.ExcercisableDate.Value.Year.ToString("0000") + "-" + security.ExcercisableDate.Value.Month.ToString("00") + "-" + security.ExcercisableDate.Value.Day.ToString("00") + " 00:00:00";
                ColumnValuePairs.Add(new KeyValuePair<string, string>("ExcercisableDate", "'" + as_date + "'"));
            }

            if (security.ExpirationDate.HasValue)
            {
                string as_date = security.ExpirationDate.Value.Year.ToString("0000") + "-" + security.ExpirationDate.Value.Month.ToString("00") + "-" + security.ExpirationDate.Value.Day.ToString("00") + " 00:00:00";
                ColumnValuePairs.Add(new KeyValuePair<string, string>("ExpirationDate", "'" + as_date + "'"));
            }

            if (security.UnderlyingSecurityTitle != null)
            {
                ColumnValuePairs.Add(new KeyValuePair<string, string>("UnderlyingSecurityTitle", "'" + security.UnderlyingSecurityTitle + "'"));
            }

            //Prepare the values
            string part_columnnames = "";
            string part_values = "";
            foreach (KeyValuePair<string, string> kvp in ColumnValuePairs)
            {
                part_columnnames = part_columnnames + kvp.Key + ",";
                part_values = part_values + kvp.Value + ",";
            }
            part_columnnames = part_columnnames.Substring(0, part_columnnames.Length-1);
            part_values = part_values.Substring(0, part_values.Length-1);

            string cmd = "insert into Security (" + part_columnnames + ") values (" + part_values + ")";

            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            return ToReturn;
        }

        #endregion

        #region "Existence checking"

        public async Task<bool> PersonExistsAsync(string cik)
        {
            string cmd = "select count(Cik) from Person where Cik='" + cik + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int Count = dr.GetInt32(0);
            sqlcon.Close();
            if (Count > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion

        #region "Matching functions"

        /// <summary>
        /// Finds a security that matches the specified criteria. Returns null if one does not exist.
        /// </summary>
        public async Task<Guid?> FindSecurityAsync(Security s)
        {

            #region "Filter string prep"

            string filter = "";

            //Add basic filter (these will be used regardless of whether this is non derivative or derivative)
            filter = "CompanyCik='" + s.Company.CIK + "' and Title='" + s.Title + "' and SecurityType=" + Convert.ToInt32(s.SecurityType).ToString();

            //Add derivative filters if it is derivative
            if (s.SecurityType == SecurityType.Derivative)
            {
                //Conversion or excercise price
                if (s.ConversionOrExcercisePrice.HasValue)
                {
                    filter = filter + " and ConversionOrExcercisePrice=" + s.ConversionOrExcercisePrice.Value.ToString();
                }
                else
                {
                    filter = filter + " and ConversionOrExcercisePrice is null";
                }
            
                //Excercisable Date
                if (s.ExcercisableDate.HasValue)
                {
                    filter = filter + " and ExcercisableDate='" + s.ExcercisableDate.Value.Year.ToString("0000") + "-" + s.ExcercisableDate.Value.Month.ToString("00") + "-" + s.ExcercisableDate.Value.Day.ToString("00") + " 00:00:00" + "'";
                }
                else
                {
                    filter = filter + " and ExcercisableDate is null";
                }

                //Expiration date
                if (s.ExpirationDate.HasValue)
                {
                    filter = filter + " and ExpirationDate='" + s.ExpirationDate.Value.Year.ToString("0000") + "-" + s.ExpirationDate.Value.Month.ToString("00") + s.ExpirationDate.Value.Day.ToString("00") + " 00:00:00'";
                }
                else
                {
                    filter = filter + " and ExpirationDate is null";
                }

                //Underlying security title
                if (s.UnderlyingSecurityTitle != null)
                {
                    filter = filter + " and UnderlyingSecurityTitle='" + s.UnderlyingSecurityTitle + "'";
                }
                else
                {
                    filter = filter + " and UnderlyingSecurityTitle is null";
                }
            }

            #endregion

            string cmd = "select Id from Security where " + filter;

            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows)
            {
                List<Guid> ToReturnOne = new List<Guid>();
                while (dr.Read())
                {
                    if (dr.IsDBNull(0) == false)
                    {
                        ToReturnOne.Add(dr.GetGuid(0));
                    }
                }
                if (ToReturnOne.Count == 1)
                {
                    sqlcon.Close();
                    return ToReturnOne[0];
                }
                else
                {
                    throw new Exception("There were multiple securities that met the specified criteria.");
                }
            }
            else
            {
                return null;
            }
        }

        #endregion

        #region "Utility functions"

        private SqlConnection GetSqlConnection()
        {
            SqlConnection sqlcon = new SqlConnection(SqlConnectionString);
            return sqlcon;
        }

        #endregion

    }
}