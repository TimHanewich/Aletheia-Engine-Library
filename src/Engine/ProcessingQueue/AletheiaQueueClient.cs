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

        public async Task<ProcessingConfiguration> GetProcessingConfigurationAsync(byte id)
        {
            string cmd = "select InternalProcessingPaused, ResumeInternalProcessingInSeconds from ProcessingConfiguration where Id = " + id.ToString();
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find ProcessingConfiguration with Id " + id.ToString());
            }
            dr.Read();
            ProcessingConfiguration ToReturn = new ProcessingConfiguration();
            ToReturn.Id = id;
            ToReturn.InternalProcessingPaused = dr.GetBoolean(0);
            ToReturn.ResumeInternalProcessingInSeconds = Convert.ToInt32(dr.GetInt16(1));
            await sqlcon.CloseAsync();
            return ToReturn;
        }

        public async Task<ProcessingConfiguration> GetPrimaryProcessingConfigurationAsync()
        {
            ProcessingConfiguration ToReturn = await GetProcessingConfigurationAsync(0);
            return ToReturn;
        }

        #region "ProcessingTask"

        public async Task UploadProcessingTaskAsync(ProcessingTask t)
        {
            string cmd = "insert into ProcessingTask(Id, AddedAtUtc, TaskType, PriorityLevel) values ('" + t.Id.ToString() + "', '" + t.AddedAtUtc.ToString() + "', " + Convert.ToInt32(t.TaskType).ToString() + ", " + t.PriorityLevel.ToString() + ")";
            await ExecuteNonQueryAsync(cmd);
        }

        //Will return null if there is nothing to process
        public async Task<ProcessingTask> RetrieveNextProcessingTaskAsync(bool and_delete_from_queue = false)
        {
            string cmd = "select top 1 Id, AddedAtUtc, TaskType, PriorityLevel from ProcessingTask order by PriorityLevel desc, AddedAtUtc desc";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            dr.Read();
            ProcessingTask ToReturn = ExtractProcessingTaskFromSqlDataReader(dr);
            sqlcon.Close();

            //Delete it from the queue now if asked to
            if (and_delete_from_queue)
            {
                await DeleteProcessingTaskAsync(ToReturn.Id);
            }

            return ToReturn;
        }

        public async Task DeleteProcessingTaskAsync(Guid id)
        {
            string cmd = "delete from ProcessingTask where Id = '" + id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd);

            //Also delete any of the children details now below

            //Delete child SecFilingTaskDetails
            string cmd2 = "delete from SecFilingTaskDetails where ParentTask = '" + id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd2);
        }

        private ProcessingTask ExtractProcessingTaskFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            ProcessingTask ToReturn = new ProcessingTask();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
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

            //TaskType
            try
            {
                ToReturn.TaskType = (TaskType)dr.GetByte(dr.GetOrdinal(prefix + "TaskType"));
            }
            catch
            {

            }

            //PriortyLevel
            try
            {
                ToReturn.PriorityLevel = Convert.ToInt32(dr.GetInt16(dr.GetOrdinal(prefix + "PriorityLevel")));
            }
            catch
            {

            }

            return ToReturn;
        }

        #endregion


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