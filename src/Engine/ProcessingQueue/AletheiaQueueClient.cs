using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Aletheia.Engine.ProcessingQueue
{
    public class AletheiaQueueClient
    {

        private string SqlConnectionString;

        public AletheiaQueueClient(string sql_connection_string)
        {
            SqlConnectionString = sql_connection_string;
        }

        public async Task InitializeTablesAsync()
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

            //ProcessingConfiguration
            if (ExistingTableNames.Contains("ProcessingConfiguration") == false)
            {
                string cmd = "create table ProcessingConfiguration (Id tinyint primary key not null, InternalProcessingPaused bit, ResumeInternalProcessingInSeconds smallint)";
                await ExecuteNonQueryAsync(cmd);
            }

            //ProcessingTask
            if (ExistingTableNames.Contains("ProcessingTask") == false)
            {
                string cmd = "create table ProcessingTask (Id uniqueidentifier primary key not null, AddedAtUtc datetime, TaskType tinyint, PriorityLevel smallint)";
                await ExecuteNonQueryAsync(cmd);
            }

            //SecFilingTaskDetails
            if (ExistingTableNames.Contains("SecFilingTaskDetails") == false)
            {
                string cmd = "create table SecFilingTaskDetails (Id uniqueidentifier primary key not null, ParentTask uniqueidentifier, FilingUrl varchar(255))";
                await ExecuteNonQueryAsync(cmd);
            }

        }

        public async Task UpdateProcessingConfigurationAsync(ProcessingConfiguration config)
        {
            bool AlreadyExists = await ProcessingConfigurationExistsAsync(config.Id);
            string cmd = "";
            if (AlreadyExists)
            {
                cmd = "update ProcessingConfiguration set InternalProcessingPaused = " + Convert.ToInt32(config.InternalProcessingPaused).ToString() + ", ResumeInternalProcessingInSeconds = " + config.ResumeInternalProcessingInSeconds.ToString() + " where Id = " + config.Id.ToString();
                
            }
            else
            {
                cmd = "insert into ProcessingConfiguration (Id, InternalProcessingPaused, ResumeInternalProcessingInSeconds) values (" + config.Id.ToString() + ", " + Convert.ToInt32(config.InternalProcessingPaused).ToString() + ", " + config.ResumeInternalProcessingInSeconds.ToString() + ")";
            }

            await ExecuteNonQueryAsync(cmd);
        }

        public async Task<bool> ProcessingConfigurationExistsAsync(byte id)
        {
            string cmd = "select count(Id) from ProcessingConfiguration where Id = " + id.ToString();
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

        // public async Task<ProcessingConfiguration> GetProcessingConfigurationAsync(byte id)
        // {
        //     string cmd = "select InternalProcessingPaused, ResumeInternalProcessingInSeconds"
        // }

        private SqlConnection GetSqlConnection()
        {
            if (SqlConnectionString == null)
            {
                throw new Exception("Unable to prepare SQL connection: connection string was null.");
            }
            else if (SqlConnectionString == " ")
            {
                throw new Exception("Unable to prepare SQL connection: connection string was blank.");
            }
            SqlConnection sqlcon = new SqlConnection(SqlConnectionString);
            return sqlcon;
        }

        private async Task ExecuteNonQueryAsync(string cmd)
        {
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            await sqlcmd.ExecuteNonQueryAsync();
            await sqlcon.CloseAsync();
        }

    }
}