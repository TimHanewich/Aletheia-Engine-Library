using System;
using System.Threading;
using System.Threading.Tasks;
using Aletheia;
using Aletheia.InsiderTrading;

namespace Aletheia.Engine.Cloud
{
    public class AletheiaComprehensiveProcessor
    {
        //Events
        public event Action ProcessingStarted;
        public event Action ProcessingComplete;
        public event StatusUpdate StatusChanged;

        //private vars
        private AletheiaCloudClient acc;

        public AletheiaComprehensiveProcessor(AletheiaCloudClient authenticated_acc)
        {
            acc = authenticated_acc;
        }

        public async Task ProcessAndUploadForm345FilingAsync(string filing_url, bool overwrite = false)
        {
            //Process it
            if (ProcessingStarted != null)
            {
                ProcessingStarted.Invoke();
            }
            TryUpdateStatus("Processing filing");
            AletheiaProcessor ap = new AletheiaProcessor();
            AletheiaProcessingResult apr;
            try
            {
                apr = await ap.ProcessStatementOfBeneficialOwnershipAsync(filing_url);
            }
            catch (Exception ex)
            {
                throw new Exception("Fatal error while attempting to process filing. Msg: " + ex.Message);
            }

            //Has this already been uploaded? If it has, should we overwrite?
            TryUpdateStatus("Checking if this filing has already been processed...");
            Guid? g = await acc.FindSecFilingAsync(apr.SecFilings[0].AccessionP1, apr.SecFilings[0].AccessionP2, apr.SecFilings[0].AccessionP3);
            if (g.HasValue)
            {
                if (overwrite == false) //It has already been processed (exists in the database) and overwrite is false, so cancel
                {
                    TryUpdateStatus("This filing has already been processed. Aborting.");
                    if (ProcessingComplete != null)
                    {
                        ProcessingComplete.Invoke();
                    }
                    return;
                }
                else //It has already been processed but overwrite is turned on, so now delete the old data and re-do it.
                {
                    TryUpdateStatus("This filing has already been processed. Will overwrite.");

                    //First, delete all Held officer positions that stemmed from this SecFiling
                    TryUpdateStatus("Deleting held officer positions that came from this filing...");
                    await acc.DeleteHeldOfficerPositionsFromFilingAsync(g.Value);
                    
                    //Now delete the transactions/holdings from the filing
                    TryUpdateStatus("Deleting SecurityTransactionHolding that came from this filing...");
                    await acc.DeleteSecurityTransactionHoldingsFromFilingAsync(g.Value);

                    //Now delete the SecFiling itself
                    TryUpdateStatus("Deleting the old SecFiling...");
                    await acc.DeleteSecFilingAsync(g.Value);

                    TryUpdateStatus("Deletion phase in preparation of overwrite complete.");
                }
            }
            else
            {
                TryUpdateStatus("Filing has not been processed yet!");
            }

            //Start with the SecEntities - do they exist in the database?
            foreach (SecEntity entity in apr.SecEntities)
            {
                TryUpdateStatus("Checking if entity " + entity.Cik.ToString() + " already exists...");
                bool EntityExists = await acc.SecEntityExistsAsync(entity.Cik);
                if (EntityExists == false)
                {
                    TryUpdateStatus("Entity " + entity.Cik.ToString() + " has not been added to the database yet. Adding...");
                    await acc.UploadSecEntityAsync(entity);
                }
                else
                {
                    TryUpdateStatus("Entity " + entity.Cik.ToString() + " already exists in the database.");
                }
            }

            //Now upload the SecFiling itself
            TryUpdateStatus("Uploading SecFiling...");
            try
            {
                await acc.UploadSecFilingAsync(apr.SecFilings[0]);
            }
            catch (Exception ex)
            {
                throw new Exception("Fatal failure while trying to upload SecFiling. Msg: " + ex.Message);
            }
            
            
            //Now upload each SecurityTransactionHolding
            int t = 1;
            foreach (SecurityTransactionHolding sth in apr.SecurityTransactionHoldings)
            {
                TryUpdateStatus("Uploading transaction #" + t.ToString() + "...");

                try
                {
                    await acc.UploadSecurityTransactionHoldingAsync(sth);
                }
                catch (Exception ex)
                {
                    throw new Exception("Fatal failure while uploading SecurityTransactionFiling #" + t.ToString() + ". Msg: " + ex.Message);
                }

                t = t + 1;
            }

            //Now upload each held officer position
            t = 1;
            foreach (HeldOfficerPosition hop in apr.HeldOfficerPositions)
            {
                TryUpdateStatus("Uploading HeldOfficerPosition #" + t.ToString());

                try
                {
                    await acc.UploadHeldOfficerPositionAsync(hop);
                }
                catch (Exception ex)
                {
                    throw new Exception("Fatal failure while uploading held officer position #" + t.ToString() + ". Msg: " + ex.Message);
                }

                t = t + 1;
            }

            //Signal the processing is complete
            if (ProcessingComplete != null)
            {
                ProcessingComplete.Invoke();
            }
        }

        private void TryUpdateStatus(string status)
        {
            if (StatusChanged != null)
            {
                StatusChanged.Invoke(status);
            }
        }
    }

    public delegate void StatusUpdate(string s);
}