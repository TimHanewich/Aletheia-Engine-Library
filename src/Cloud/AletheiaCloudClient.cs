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
                string cmd = "create table Security (Id uniqueidentifier not null primary key, CompanyCik char(10), Title varchar(255), SecurityType bit)";
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
            string cmd = "insert into Person (Cik,FullName) values (" + p.CIK + "," + p.FullName + ")";
            SqlConnection sqlcon = GetSqlConnection();
            await sqlcon.OpenAsync();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            sqlcon.Close();
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