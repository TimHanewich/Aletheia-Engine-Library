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
                string cmd = "create table SecurityTransaction (Id uniqueidentifier not null primary key, OwnedBy char(10), SecurityId uniqueidentifier, SecAccessionNumber char(20), AcquiredDisposed bit, Quantity real, TransactionDate datetime, TransactionCode tinyint, QuantityOwnedFollowingTransaction real, DirectIndirect bit, ReportedOn datetime)";
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
            ColumnValuePairs.Add(new KeyValuePair<string, string>("ReportedOn", "'" + transaction.ReportedOn.Year.ToString("0000") + "-" + transaction.ReportedOn.Month.ToString("00") + "-" + transaction.ReportedOn.Day.ToString("00") + " 00:00:00'"));

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

        public async Task UploadCompanyAsync(Company c)
        {
            string cmd = "insert into Company (Cik,TradingSymbol,Name) values ('" + c.CIK + "','" + c.TradingSymbol + "','" + c.Name + "')";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
        }

        //DOWNLOADING BELOW

        public async Task<Person> DownloadPersonAsync(string cik)
        {
            string cmd = "select Cik, FullName from Person where Cik = '" + cik + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find person with CIK '" + cik + "'");
            }
            await dr.ReadAsync();
            Person ToReturn = new Person();
            if (dr.IsDBNull(0) == false)
            {
                ToReturn.CIK = dr.GetString(0);
            }
            if (dr.IsDBNull(1) == false)
            {
                ToReturn.FullName = dr.GetString(1);
            }
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<Company> DownloadCompanyByCikAsync(string cik)
        {
            string cmd = "select TradingSymbol, Name from Company where Cik = '" + cik + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find company with CIK '" + cik + "'");
            }
            await dr.ReadAsync();
            Company ToReturn = new Company();
            ToReturn.CIK = cik;
            if (dr.IsDBNull(0) == false)
            {
                ToReturn.TradingSymbol = dr.GetString(0);
            }
            if (dr.IsDBNull(1) == false)
            {
                ToReturn.Name = dr.GetString(1);
            }
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<Company> DownloadCompanyBySymbolAsync(string symbol)
        {
            string cmd = "select Cik, Name from Company where TradingSymbol='" + symbol.Trim().ToUpper() + "'"; //Do not have to retrieve the trading symbol because that is supplied as a parameter.
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            //Get the results
            List<Company> OneToReturn = new List<Company>();
            while (dr.Read())
            {
                Company c = new Company();
                c.CIK = dr.GetString(0);
                c.TradingSymbol = symbol.Trim().ToUpper();
                c.Name = dr.GetString(1);
                OneToReturn.Add(c);
            }

            //Close the connection
            sqlcon.Close();

            //Throw an error if there is 0 or there are more than 1
            if (OneToReturn.Count == 0)
            {
                throw new Exception("Unable to find company with symbol '" + symbol.Trim().ToUpper() + "'");
            }
            else if (OneToReturn.Count > 1)
            {
                throw new Exception("There were multiple companies with symbol '" + symbol.Trim().ToUpper() + "'");
            }
            
            return OneToReturn[0];
        }

        public async Task<Security> CascadeDownloadSecurityAsync(Guid id)
        {
            string cmd = "select CompanyCik, Title, SecurityType, ConversionOrExcercisePrice, ExcercisableDate, ExpirationDate, UnderlyingSecurityTitle from Security where Id = '" + id.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find security with ID '" + id.ToString() + "'");
            }
            await dr.ReadAsync();

            Security ToReturn = new Security();

            //Get the company
            if (dr.IsDBNull(0) == false)
            {
                string companycikstr = dr.GetString(0);
                Company c = await DownloadCompanyByCikAsync(companycikstr);
                ToReturn.Company = c;
            }

            //Get the title
            if (dr.IsDBNull(1) == false)
            {
                ToReturn.Title = dr.GetString(1);
            }

            //Get the security type
            if (dr.IsDBNull(2) == false)
            {
                bool SecTypeValue = dr.GetBoolean(2);
                if (SecTypeValue == false)
                {
                    ToReturn.SecurityType = SecurityType.NonDerivative;
                }
                else
                {
                    ToReturn.SecurityType = SecurityType.Derivative;
                }
            }

            //Conversion of excercise price
            if (dr.IsDBNull(3) == false)
            {
                ToReturn.ConversionOrExcercisePrice = dr.GetFloat(3);
            }

            //Excercisable date
            if (dr.IsDBNull(4) == false)
            {
                ToReturn.ExcercisableDate = dr.GetDateTime(4);
            }

            //Expiration date
            if (dr.IsDBNull(5) == false)
            {
                ToReturn.ExpirationDate = dr.GetDateTime(5);
            }

            //Underlying security title
            if (dr.IsDBNull(6) == false)
            {
                ToReturn.UnderlyingSecurityTitle = dr.GetString(0);
            }

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

        public async Task<bool> CompanyExistsAsync(string cik)
        {
            string cmd = "select count(Cik) from Company where Cik='" + cik + "'";
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

        /// <summary>
        /// Used for checking if a particular filing has already been logged. Checks if any security transactions have this accession number.
        /// </summary>
        public async Task<bool> SecurityTransactionsWithAccessionNumberExistAsync(string accession_number)
        {
            string cmd = "select count(Id) from SecurityTransaction where SecAccessionNumber='" + accession_number + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int Val = dr.GetInt32(0);
            sqlcon.Close();
            if (Val > 0)
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

            //Put them into a list
            List<Guid> MatchingSecurities = new List<Guid>();
            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    if (dr.IsDBNull(0) == false)
                    {
                        MatchingSecurities.Add(dr.GetGuid(0));
                    }
                }
            }

            //Close the sql connection
            sqlcon.Close();

            //Return
            if (MatchingSecurities.Count == 0)
            {
                return null;   
            }
            else if (MatchingSecurities.Count == 1)
            {
                return MatchingSecurities[0];
            }
            else 
            {
                throw new Exception("There were multiple securities that met the specified criteria.");
            }
        }

        #endregion

        #region "Cascade methods"

        public async Task<Guid> CascadeUploadSecurityTransactionAsync(SecurityTransaction st)
        {
            //Upload person if it does not exist
            bool PersonExists = await PersonExistsAsync(st.OwnedBy.CIK);
            if (PersonExists == false)
            {
                await UploadPersonAsync(st.OwnedBy);
            }

            //Upload the company if it does not exist
            bool CompanyExists = await CompanyExistsAsync(st.SubjectSecurity.Company.CIK);
            if (CompanyExists == false)
            {
                await UploadCompanyAsync(st.SubjectSecurity.Company);
            }

            //Upload the security if it does not exist
            Guid? ThisSecurityId = await FindSecurityAsync(st.SubjectSecurity);
            if (ThisSecurityId.HasValue == false)
            {
                ThisSecurityId = await UploadSecurityAsync(st.SubjectSecurity);
            }

            //Upload the security transaction
            Guid ThisSecurityTransactionId = await UploadSecurityTransactionAsync(st);
            
            //Plug in the references: OwnedBy and SecurityId
            string cmd = "update SecurityTransaction set OwnedBy='" + st.OwnedBy.CIK + "', SecurityId='" + ThisSecurityId.Value.ToString() + "' where Id='" + ThisSecurityTransactionId.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();

            return ThisSecurityTransactionId;
        }

        #endregion

        #region "Delete methods"

        public async Task<int> DeleteSecurityTransactionsByAccessionNumberAsync(string accession_number)
        {
            string cmd = "delete from SecurityTransaction where SecAccessionNumber='" + accession_number + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            int ToReturn = await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
            return ToReturn;
        }

        #endregion

        #region "Advanced methods - Get most recent security transactions"

        public async Task<SecurityTransaction[]> GetSecurityTransactionsAsync(EntityType search_for, string cik, int top = 20, DateTime? before = null, SecurityType? security_type = null, SecuritiesExchangeCommission.Edgar.TransactionType? transaction_type = null)
        {
            //Establish the where filter
            string WhereFilter = "";

            //Where filter step 1 - Establish the where filter for the company filter
            if (search_for == EntityType.Company)
            {
                WhereFilter = "Company.Cik = '" + cik + "'";
            }
            else if (search_for == EntityType.Person)
            {
                WhereFilter = "Person.Cik = '" + cik + "'";
            }
            
            //Add date filter if it is provided
            if (before.HasValue)
            {
                WhereFilter = WhereFilter + " and SecurityTransaction.TransactionDate < '" + before.Value.Year.ToString("0000") + "-" + before.Value.Month.ToString("00") + "-" + before.Value.Day.ToString("00") + "'";
            }

            //Security Type? Only if provided
            if (security_type.HasValue)
            {
                WhereFilter = WhereFilter + " and Security.SecurityType = " + Convert.ToInt32(security_type.Value).ToString();
            }
            
            //Transaction type? Only if provided
            if (transaction_type.HasValue)
            {
                WhereFilter = WhereFilter + " and SecurityTransaction.TransactionCode = " + Convert.ToInt32(transaction_type).ToString();
            }

            SecurityTransaction[] transactions = await CascadeDownloadSecurityTransactionsFromWhereFilterAsync(top, WhereFilter);
            return transactions;
        }

        //This is used by the methods for getting most recent transactions for a company and a peron
        private async Task<SecurityTransaction[]> CascadeDownloadSecurityTransactionsFromWhereFilterAsync(int top, string where_filter)
        {
            string columns = "SecurityTransaction.SecAccessionNumber, SecurityTransaction.AcquiredDisposed, SecurityTransaction.Quantity, SecurityTransaction.TransactionDate, SecurityTransaction.TransactionCode, SecurityTransaction.QuantityOwnedFollowingTransaction, SecurityTransaction.DirectIndirect, SecurityTransaction.ReportedOn, Person.Cik, Person.FullName, Security.Title, Security.SecurityType, Security.ConversionOrExcercisePrice, Security.ExcercisableDate, Security.ExpirationDate, Security.UnderlyingSecurityTitle, Company.Cik, Company.TradingSymbol, Company.Name";
            string cmd = "select top " + top.ToString() + " " + columns + " from SecurityTransaction inner join Person on SecurityTransaction.OwnedBy = Person.Cik inner join Security on SecurityTransaction.SecurityId = Security.Id inner join Company on Security.CompanyCik = Company.Cik where " + where_filter + " order by SecurityTransaction.TransactionDate desc";

            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            //Parse each of them
            List<SecurityTransaction> ToReturn = new List<SecurityTransaction>();
            while (dr.Read())
            {
                SecurityTransaction ThisTransaction = new SecurityTransaction();

                //Sec Accession Number
                if (dr.IsDBNull(0) == false)
                {
                    ThisTransaction.SecAccessionNumber = dr.GetString(0);
                }

                //Acquired Disposed
                if (dr.IsDBNull(1) == false)
                {
                    bool AD_Val = dr.GetBoolean(1);
                    if (AD_Val == false)
                    {
                        ThisTransaction.AcquiredDisposed = SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Acquired;
                    }
                    else
                    {
                        ThisTransaction.AcquiredDisposed = SecuritiesExchangeCommission.Edgar.AcquiredDisposed.Disposed;
                    }
                }

                //Quantity
                if (dr.IsDBNull(2) == false)
                {
                    ThisTransaction.Quantity = dr.GetFloat(2);
                }

                //Transaction date
                if (dr.IsDBNull(3) == false)
                {
                    ThisTransaction.TransactionDate = dr.GetDateTime(3);
                }

                //Transaction code
                if (dr.IsDBNull(4) == false)
                {
                    ThisTransaction.TransactionCode = (SecuritiesExchangeCommission.Edgar.TransactionType)dr.GetByte(4);
                }

                //Quantity owned following transaction
                if (dr.IsDBNull(5) == false)
                {
                    ThisTransaction.QuantityOwnedFollowingTransaction = dr.GetFloat(5);
                }

                //Direct or indirect
                if (dr.IsDBNull(6) == false)
                {
                    bool val = dr.GetBoolean(6);
                    if (val == false)
                    {
                        ThisTransaction.DirectIndirect = SecuritiesExchangeCommission.Edgar.OwnershipNature.Direct;
                    }
                    else
                    {
                        ThisTransaction.DirectIndirect = SecuritiesExchangeCommission.Edgar.OwnershipNature.Indirect;
                    }
                }

                //Reported on
                if (dr.IsDBNull(7) == false)
                {
                    ThisTransaction.ReportedOn = dr.GetDateTime(7);
                }

                #region "Person (OwnedBy)"

                Person p = new Person();

                if (dr.IsDBNull(8) == false)
                {
                    p.CIK = dr.GetString(8);
                }

                if (dr.IsDBNull(9) == false)
                {
                    p.FullName = dr.GetString(9);
                }

                ThisTransaction.OwnedBy = p;

                #endregion

                #region "Security - must happen before Company"

                Security s = new Security();

                //Title
                if (dr.IsDBNull(10) == false)
                {
                    s.Title = dr.GetString(10);
                }

                //Security Type
                if (dr.IsDBNull(11) == false)
                {
                    bool val = dr.GetBoolean(11);
                    if (val == false)
                    {
                        s.SecurityType = SecurityType.NonDerivative;
                    }
                    else
                    {
                        s.SecurityType = SecurityType.Derivative;
                    }
                }

                //Security Conversion or excercisable price
                if (dr.IsDBNull(12) == false)
                {
                    s.ConversionOrExcercisePrice = dr.GetFloat(12);
                }

                //Excercisable Date
                if (dr.IsDBNull(13) == false)
                {
                    s.ExcercisableDate = dr.GetDateTime(13);
                }

                //Expiration date
                if (dr.IsDBNull(14) == false)
                {
                    s.ExpirationDate = dr.GetDateTime(14);
                }

                //Underlying security title
                if (dr.IsDBNull(15) == false)
                {
                    s.UnderlyingSecurityTitle = dr.GetString(15);
                }

                ThisTransaction.SubjectSecurity = s;

                #endregion

                #region "Company - must happen after security"

                Company c = new Company();

                //Company CIK
                if (dr.IsDBNull(16) == false)
                {
                    c.CIK = dr.GetString(16);
                }

                //Company Trading symbol
                if (dr.IsDBNull(17) == false)
                {
                    c.TradingSymbol = dr.GetString(17);
                }

                //Company name
                if (dr.IsDBNull(18) == false)
                {
                    c.Name = dr.GetString(18);
                }

                //Plug it into the security
                ThisTransaction.SubjectSecurity.Company = c;

                #endregion

                ToReturn.Add(ThisTransaction);
            }

            sqlcon.Close();

            return ToReturn.ToArray();
        }

        #endregion

        #region "Advanced methods - Get securities for a company or a person"

        public async Task<Security[]> GetSecuritiesByCompanyAsync(string company_cik_or_symbol)
        {
            //is it a cik or symbol (Cik will be a number)
            bool IsCik = false;
            try
            {
                int.Parse(company_cik_or_symbol);
                IsCik = true;
            }
            catch
            {
                IsCik = false;
            }
        
            //Prepare the where filter
            string wherefilter = "";
            if (IsCik)
            {
                wherefilter = "Company.Cik = '" + company_cik_or_symbol + "'";
            }
            else
            {
                wherefilter = "Company.TradingSymbol = '" + company_cik_or_symbol.ToUpper() + "'";
            }

            Security[] ToReturn = await GetSecuritiesByWhereFilterAsync(wherefilter);
            return ToReturn;
        }

        public async Task<Security[]> GetSecuritiesOwnedByPersonAsync(string person_cik)
        {
            string wherefilter = "Person.Cik = '" + person_cik + "'";
            Security[] ToReturn = await GetSecuritiesByWhereFilterAsync(wherefilter);
            return ToReturn;
        }

        private async Task<Security[]> GetSecuritiesByWhereFilterAsync(string where_filter)
        {
            string cmd = "select distinct Security.Title, Security.SecurityType, Security.ConversionOrExcercisePrice, Security.ExcercisableDate, Security.ExpirationDate, Security.UnderlyingSecurityTitle, Company.Cik, Company.Name, Company.TradingSymbol from SecurityTransaction inner join Person on SecurityTransaction.OwnedBy = Person.Cik inner join Security on SecurityTransaction.SecurityId = Security.Id inner join Company on Security.CompanyCik = Company.Cik where " + where_filter;
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            List<Security> ToReturn = new List<Security>();
            while (dr.Read())
            {
                Security ThisSecurity = new Security();

                //Title
                if (dr.IsDBNull(0) == false)
                {
                    ThisSecurity.Title = dr.GetString(0);
                }

                //Security Type
                if (dr.IsDBNull(1) == false)
                {
                    bool val = dr.GetBoolean(1);
                    if (val == false)
                    {
                        ThisSecurity.SecurityType = SecurityType.NonDerivative;
                    }
                    else
                    {
                        ThisSecurity.SecurityType = SecurityType.Derivative;
                    }
                }

                //Conversion or excercise price
                if (dr.IsDBNull(2) == false)
                {
                    ThisSecurity.ConversionOrExcercisePrice = dr.GetFloat(2);
                }

                //Excercisable date
                if (dr.IsDBNull(3) == false)
                {
                    ThisSecurity.ExcercisableDate = dr.GetDateTime(3);
                }

                //Expiration date
                if (dr.IsDBNull(4) == false)
                {
                    ThisSecurity.ExpirationDate = dr.GetDateTime(4);
                }

                //Underlying security title
                if (dr.IsDBNull(5) == false)
                {
                    ThisSecurity.UnderlyingSecurityTitle = dr.GetString(5);
                }

                #region "Company"

                Company c = new Company();

                if (dr.IsDBNull(6) == false)
                {
                    c.CIK = dr.GetString(6);
                }

                if (dr.IsDBNull(7) == false)
                {
                    c.Name = dr.GetString(7);
                }

                if (dr.IsDBNull(8) == false)
                {
                    c.TradingSymbol = dr.GetString(8);
                }

                ThisSecurity.Company = c;

                #endregion

                ToReturn.Add(ThisSecurity);
            }
            
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        #endregion

        #region "Advanced methods - Search"

        public async Task<Company[]> SearchCompaniesAsync(string search_term)
        {
            string cmd = "select top 20 Cik, TradingSymbol, Name from Company where Cik like '%" + search_term + "%' or TradingSymbol like '%" + search_term + "%' or Name like '%" + search_term + "%'";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();

            //Get the results
            List<Company> ToReturn = new List<Company>();
            while (dr.Read())
            {
                Company ThisCompany = new Company();

                //CIK
                if (dr.IsDBNull(0) == false)
                {
                    ThisCompany.CIK = dr.GetString(0);
                }

                //Trading symbol
                if (dr.IsDBNull(1) == false)
                {
                    ThisCompany.TradingSymbol = dr.GetString(1);
                }

                //Name
                if (dr.IsDBNull(2) == false)
                {
                    ThisCompany.Name = dr.GetString(2);
                }

                ToReturn.Add(ThisCompany);
            }
        
            sqlcon.Close();

            return ToReturn.ToArray();
        }

        public async Task<Person[]> SearchPeopleAsync(string search_term)
        {
            string cmd = "select top 20 Cik, FullName from Person where Cik like '%" + search_term + "%' or FullName like '%" + search_term + "%'";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            
            //Read
            List<Person> ToReturn = new List<Person>();
            while (dr.Read())
            {
                Person ThisPerson = new Person();
                if (dr.IsDBNull(0) == false)
                {
                    ThisPerson.CIK = dr.GetString(0);
                }
                if (dr.IsDBNull(1) == false)
                {
                    ThisPerson.FullName = dr.GetString(1);
                }
                ToReturn.Add(ThisPerson);
            }

            sqlcon.Close();
            return ToReturn.ToArray();
        }

        #endregion

        #region "DB Statistic methods"

        public async Task<int> CountSecurityTransactionsAsync()
        {
            string cmd = "select count(Id) from SecurityTransaction";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountSecuritiesAsync()
        {
            string cmd = "select count(Id) from Security";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountNonDerivativeSecuritiesAsync()
        {
            string cmd = "select count(Id) from Security where SecurityType = 0";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountDerivativeSecuritiesAsync()
        {
            string cmd = "select count(Id) from Security where SecurityType = 1";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountCompaniesAsync()
        {
            string cmd = "select count(Cik) from Company";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountPeopleAsync()
        {
            string cmd = "select count(Cik) from Person";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
        }

        public async Task<int> CountSecForm4DocumentsProcessedAsync()
        {
            string cmd = "select count(distinct SecAccessionNumber) from SecurityTransaction";
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

        #region "Utility functions"

        private SqlConnection GetSqlConnection()
        {
            SqlConnection sqlcon = new SqlConnection(SqlConnectionString);
            return sqlcon;
        }

        #endregion

    }
}