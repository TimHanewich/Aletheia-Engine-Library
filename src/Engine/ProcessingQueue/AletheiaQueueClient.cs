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
            sqlcon.Close();
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
            string cmd = "insert into ProcessingTask(Id, AddedAtUtc, TaskType, PriorityLevel, AttemptedAndFailed) values ('" + t.Id.ToString() + "', '" + t.AddedAtUtc.ToString() + "', " + Convert.ToInt32(t.TaskType).ToString() + ", " + t.PriorityLevel.ToString() + ", " + Convert.ToInt32(t.AttemptedAndFailed).ToString() + ")";
            await ExecuteNonQueryAsync(cmd);
        }

        //Will return null if there is nothing to process
        public async Task<ProcessingTask> RetrieveNextProcessingTaskAsync(bool and_delete_from_queue = false)
        {
            string cmd = "select top 1 Id, AddedAtUtc, TaskType, PriorityLevel, from ProcessingTask order by PriorityLevel desc, AddedAtUtc asc";
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

            //AttemptedAndFailed
            try
            {
                ToReturn.AttemptedAndFailed = dr.GetBoolean(dr.GetOrdinal(prefix + "AttemptedAndFailed"));
            }
            catch
            {
                ToReturn.AttemptedAndFailed = false;
            }

            return ToReturn;
        }

        #endregion

        #region "SecFilingTaskDetails"

        public async Task UploadSecFilingTaskDetailsAsync(SecFilingTaskDetails d)
        {
            string cmd = "insert into SecFilingTaskDetails (Id, ParentTask, FilingUrl) values ('" + d.Id.ToString() + "', '" + d.ParentTask.ToString() + "', '" + d.FilingUrl + "')";
            await ExecuteNonQueryAsync(cmd);
        }

        public async Task<SecFilingTaskDetails> GetSecFilingTaskDetailsAsync(Guid id)
        {
            string cmd = "select Id, ParentTask, FilingUrl from SecFilingTaskDetails where Id = '" + id.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find SecFilingTaskDetails with Id '" + id.ToString() + "'");
            }
            dr.Read();
            SecFilingTaskDetails ToReturn = ExtractSecFilingTaskDetailsFromSqlDataReader(dr);
            sqlcon.Close();
            return ToReturn;
        }

        public async Task<SecFilingTaskDetails[]> GetAssociatedSecFilingTaskDetailsAsync(Guid parent_task_id)
        {
            string cmd = "select Id, FilingUrl from SecFilingTaskDetails where ParentTask = '" + parent_task_id + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            
            List<SecFilingTaskDetails> ToReturn = new List<SecFilingTaskDetails>();
            while (dr.Read())
            {
                SecFilingTaskDetails dets = ExtractSecFilingTaskDetailsFromSqlDataReader(dr);
                dets.ParentTask = parent_task_id;
                ToReturn.Add(dets);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        private SecFilingTaskDetails ExtractSecFilingTaskDetailsFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            SecFilingTaskDetails ToReturn = new SecFilingTaskDetails();

            //Id
            try
            {
                ToReturn.Id = dr.GetGuid(dr.GetOrdinal(prefix + "Id"));
            }
            catch
            {

            }

            //ParentTask
            try
            {
                ToReturn.ParentTask = dr.GetGuid(dr.GetOrdinal(prefix + "ParentTask"));
            }
            catch
            {

            }

            //FilingUrl
            try
            {
                ToReturn.FilingUrl = dr.GetString(dr.GetOrdinal(prefix + "FilingUrl"));
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
            sqlcon.Close();
        }

    }
}