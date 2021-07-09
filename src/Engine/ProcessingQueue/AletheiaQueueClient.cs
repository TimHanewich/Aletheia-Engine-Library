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
        public async Task<ProcessingTask> RetrieveNextProcessingTaskAsync(bool? attempted_and_failed, TaskType? task_type)
        {
            //Assemble the command
            List<string> cmd = new List<string>();
            cmd.Add("select");
            cmd.Add("top 1");
            cmd.Add("Id, AddedAtUtc, TaskType, PriorityLevel, AttemptedAndFailed");
            cmd.Add("from ProcessingTask");

            #region "Where Statements"
            
            List<string> WhereStatements = new List<string>();

            if (attempted_and_failed.HasValue)
            {
                if (attempted_and_failed == false)
                {
                    WhereStatements.Add("(AttemptedAndFailed = 0 or AttemptedAndFailed is null)");
                }
                else
                {
                    WhereStatements.Add("AttemptedAndFailed = 1");
                }
            }
            
            //Filter to a task type?
            if (task_type.HasValue)
            {
                WhereStatements.Add("TaskType = " + Convert.ToInt32(task_type.Value).ToString());
            }

            //Assemble if there are any
            if (WhereStatements.Count > 0)
            {
                cmd.Add("where");
                foreach (string s in WhereStatements)
                {
                    cmd.Add(s);
                    cmd.Add("and");
                }
                cmd.RemoveAt(cmd.Count-1); //Remove the last trailing and
            }

            #endregion
            
            //Sort
            cmd.Add("order by PriorityLevel desc, AddedAtUtc asc");

            //Assemble the full command
            string cmdstr = "";
            foreach (string s in cmd)
            {
                cmdstr = cmdstr + s + Environment.NewLine;
            }

            //Call and return
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmdstr, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                return null;
            }
            dr.Read();
            ProcessingTask ToReturn = ExtractProcessingTaskFromSqlDataReader(dr);
            sqlcon.Close();

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

            //Delete child TheMotleyFoolEarningsCallTranscriptDetails
            string cmd3 = "delete from TheMotleyFoolEarningsCallTranscriptDetails where ParentTask = '" + id.ToString() + "'";
            await ExecuteNonQueryAsync(cmd3);
        }

        public async Task SetProcessingTaskAttemptedAndFailedStatusAsync(Guid processing_task_id, bool attempted_and_failed)
        {
            string sql = "update ProcessingTask set AttemptedAndFailed = " + Convert.ToInt32(attempted_and_failed).ToString() + " where Id = '" + processing_task_id + "'";
            await ExecuteNonQueryAsync(sql);
        }

        public async Task<int> CountAttemptedAndFailedProcessingTasksAsync()
        {
            string sql = "select count(Id) from ProcessingTask where AttemptedAndFailed = 1";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(sql, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            await dr.ReadAsync();
            int val = dr.GetInt32(0);
            sqlcon.Close();
            return val;
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

        #region "TheMotleyFoolEarningsCallTranscriptDetaisl"

        public async Task UploadTheMotleyFoolEarningsCallTranscriptDetailsAsync(TheMotleyFoolEarningsCallTranscriptDetails trans)
        {
            string cmd = "insert into TheMotleyFoolEarningsCallTranscriptDetails (Id, ParentTask, Url) values ('" + trans.Id.ToString() + "', '" + trans.ParentTask.ToString() + "', '" + trans.Url + "')";
            await ExecuteNonQueryAsync(cmd);
        }

        public async Task<TheMotleyFoolEarningsCallTranscriptDetails> GetTheMotleyFoolEarningsCallTranscriptDetailsAsync(Guid id)
        {
            string cmd = "select ParentTask, Url from TheMotleyFoolEarningsCallTranscriptDetails where Id = '" + id.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            if (dr.HasRows == false)
            {
                sqlcon.Close();
                throw new Exception("Unable to find TMF transcript details with Id '" + id.ToString() + "'");
            }
            await dr.ReadAsync();
            TheMotleyFoolEarningsCallTranscriptDetails ToReturn = ExtractTmfEarningsCallTranscriptDetailsFromSqlDataReader(dr, "");
            ToReturn.Id = id;
            sqlcon.Close();
            return ToReturn;
        }

        private TheMotleyFoolEarningsCallTranscriptDetails ExtractTmfEarningsCallTranscriptDetailsFromSqlDataReader(SqlDataReader dr, string prefix = "")
        {
            TheMotleyFoolEarningsCallTranscriptDetails ToReturn = new TheMotleyFoolEarningsCallTranscriptDetails();

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

            //Url
            try
            {
                ToReturn.Url = dr.GetString(dr.GetOrdinal(prefix + "Url"));
            }
            catch
            {

            }

            return ToReturn;
        }

        public async Task<TheMotleyFoolEarningsCallTranscriptDetails[]> GetAssociatedTheMotleyFoolEarningsCallTranscriptDetailsAsync(Guid parent_task_id)
        {
            string cmd = "select Id, Url from TheMotleyFoolEarningsCallTranscriptDetails where ParentTask = '" + parent_task_id.ToString() + "'";
            SqlConnection sqlcon = GetSqlConnection();
            sqlcon.Open();
            SqlCommand sqlcmd = new SqlCommand(cmd, sqlcon);
            SqlDataReader dr = await sqlcmd.ExecuteReaderAsync();
            List<TheMotleyFoolEarningsCallTranscriptDetails> ToReturn = new List<TheMotleyFoolEarningsCallTranscriptDetails>();
            while (dr.Read())
            {
                TheMotleyFoolEarningsCallTranscriptDetails ex = ExtractTmfEarningsCallTranscriptDetailsFromSqlDataReader(dr);
                ex.ParentTask = parent_task_id;
                ToReturn.Add(ex);
            }
            sqlcon.Close();
            return ToReturn.ToArray();
        }

        public async Task<bool> EarningsCallTranscriptTaskWithUrlExistsAsync(string url)
        {
            string cmd = "select count(Url) from TheMotleyFoolEarningsCallTranscriptDetails where Url = '" + url + "'";
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