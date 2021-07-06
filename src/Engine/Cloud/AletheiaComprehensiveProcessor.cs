using System;
using System.Threading;
using System.Threading.Tasks;
using Aletheia;
using Aletheia.InsiderTrading;
using SecuritiesExchangeCommission.Edgar;
using Aletheia.Fundamentals;
using System.IO;
using Xbrl;
using Xbrl.FinancialStatement;
using TheMotleyFool.Transcripts;
using Aletheia.Engine.EarningsCalls;

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

        /// <summary>
        /// Process any SEC filing. Will be routed to the proper form type processor.
        /// </summary>
        public async Task ProcessSecFilingAsync(string filing_url, bool overwrite)
        {
            EdgarFiling ef = new EdgarFiling();
            ef.DocumentsUrl = filing_url;
            TryUpdateStatus("Checking form type for routing...");
            EdgarFilingDetails efd = await ef.GetFilingDetailsAsync();
            string form = efd.Form.Trim().ToLower();
            if (form == "3" || form == "3/a" || form == "4" || form == "4/a" || form == "5" || form == "5/a") //Insider trading form
            {
                TryUpdateStatus("Routing to insider trading processing.");
                await ProcessAndUploadForm345FilingAsync(filing_url, overwrite);
            } 
            else if (form == "10-q" || form == "10-q/a" || form == "10-k" || form == "10-k/a")
            {
                TryUpdateStatus("Routing to fundamentals processing.");
                await ProcessAndUploadFundamentalsFilingAsync(filing_url, overwrite);
            }
            else
            {
                throw new Exception("Unable to process form type '" + form + "'");
            }
        }

        public async Task ProcessTheMotleyFoolEarningsCallTranscriptAsync(string tmf_url, bool overwrite)
        {
            try
            {
                ProcessingStarted.Invoke();
            }
            catch
            {

            }

            //Check if this has been done already
            TryUpdateStatus("Checking for overwrite.");
            Guid? AlreadyProcessedThis = await acc.EarningsCallExistsAsync(tmf_url);
            if (AlreadyProcessedThis.HasValue)
            {
                if (overwrite == false)
                {
                    try
                    {
                        ProcessingComplete.Invoke();
                    }
                    catch
                    {

                    }
                    TryUpdateStatus("Earnings call at '" + tmf_url + "' has already been processed.");
                    return;
                }
                else //If overwrite is on, delete everything!
                {
                    TryUpdateStatus("This transcript has already been processed but overwrite is on. Will delete!");

                    //First delete all spoken remark highlights
                    TryUpdateStatus("Deleting all associated SpokenRemarkHighlight...");
                    await acc.DeleteSpokenRemarkHighlightsFromEarningsCallAsync(AlreadyProcessedThis.Value);

                    //Now delete all of the spoken remarks
                    TryUpdateStatus("Deleting all SpokenRemark...");
                    await acc.DeleteSpokenRemarksFromEarningsCallAsync(AlreadyProcessedThis.Value);

                    //Now delete the earnings call itself
                    TryUpdateStatus("Deleting earnings call...");
                    await acc.DeleteEarningsCallAsync(AlreadyProcessedThis.Value);
                }
            }

            //Move on with processing
            TryUpdateStatus("Moving onto processing!");
            AletheiaProcessor ap = new AletheiaProcessor();
            AletheiaEarningsCallProcessingResult ecpr = await ap.ProcessEarningsCallAsync(tmf_url);

            //Upload each call company
            TryUpdateStatus("Moving on to uploading call companies");
            foreach (CallCompany cc in ecpr.CallCompanies)
            {
                TryUpdateStatus("Checking if CallCompany with trading symbol '" + cc.TradingSymbol + "' already exists in the DB.");
                Guid? exists = await acc.CallCompanyExistsAsync(cc.TradingSymbol);
                if (exists.HasValue)
                {
                    TryUpdateStatus("CallCompany with trading symbol '" + cc.TradingSymbol + "' already exists. Going to update ID's that reference this.");
                    
                    //Update all references of this to the new ID
                    foreach (EarningsCall ec in ecpr.EarningsCalls)
                    {
                        if (ec.ForCompany == cc.Id)
                        {
                            ec.ForCompany = exists.Value;
                        }
                    }

                    //Update the item itself (not necessary, but doing it just for organization)
                    cc.Id = exists.Value;
                }
                else
                {
                    TryUpdateStatus("CallCompany with trading symbol '" + cc.TradingSymbol + "' does not exists. Uploading!");
                    await acc.UploadCallCompanyAsync(cc);
                }
            }

            //Upload each earnings call
            TryUpdateStatus("Moving onto uploading earnings calls.");
            foreach (EarningsCall ec in ecpr.EarningsCalls)
            {
                TryUpdateStatus("Uploading earnings call '" + ec.Id.ToString() + "'...");
                await acc.UploadEarningsCallAsync(ec);
            }

            //Upload Call Participants
            TryUpdateStatus("Moving onto upload call participants");
            foreach (Aletheia.Engine.EarningsCalls.CallParticipant cp in ecpr.CallParticipants)
            {
                TryUpdateStatus("Checking if CallParticipant " + cp.Name + ", " + cp.Title + ", exists in the DB.");
                Guid? oid = await acc.CallParticipantExistsAsync(cp);
                if (oid.HasValue) //It exists! So change it up
                {
                    TryUpdateStatus("CallParticipant exists! Changing references.");

                    //Change each SpokenRemark that references this participant
                    foreach (SpokenRemark sr in ecpr.SpokenRemarks)
                    {
                        if (sr.SpokenBy == cp.Id)
                        {
                            sr.SpokenBy = oid.Value;
                        }
                    }

                    //Change the Id itself here (not necessary, but doing it just for organization)
                    cp.Id = oid.Value;
                }
                else //It does not exist! So upload it
                {
                    TryUpdateStatus("CallParticipant " + cp.Name + " does not exist. Uploading...");
                    await acc.UploadCallParticipantAsync(cp);
                }
            }

            //Upoad each spoken remark
            TryUpdateStatus("Moving onto uploading spoken remarks");
            int t = 1;
            foreach (SpokenRemark sr in ecpr.SpokenRemarks)
            {
                float pc = Convert.ToSingle(t) / Convert.ToSingle(ecpr.SpokenRemarks.Length);
                TryUpdateStatus("Uploading spoken remark " + t.ToString("#,##0") + " / " + ecpr.SpokenRemarks.Length.ToString("#,##0") + " (" + pc.ToString("#0.0%") + ") '" + sr.Id + "'");
                await acc.UploadSpokenRemarkAsync(sr);
                t = t + 1;
            }

            //Upload each spoken remark highlight
            TryUpdateStatus("Moving onto uploading spoken remark highlights.");
            foreach (SpokenRemarkHighlight srh in ecpr.SpokenRemarkHighlights)
            {
                TryUpdateStatus("Uploading spoken remark highlight '" + srh.Id.ToString() + "'");
                await acc.UploadSpokenRemarkHighlightAsync(srh);
            }

            //Mark as complete!
            try
            {
                ProcessingComplete.Invoke();
            }
            catch
            {
                
            }
        }

        private async Task ProcessAndUploadForm345FilingAsync(string filing_url, bool overwrite = false)
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
            Guid[] g = await acc.FindSecFilingAsync(apr.SecFilings[0].AccessionP1, apr.SecFilings[0].AccessionP2, apr.SecFilings[0].AccessionP3);
            if (g.Length > 0)
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
                    TryUpdateStatus(g.Length.ToString("#,##0") + " records of this filings exist in the database. Will cascade delete each as part of the overwrite process.");
                    
                    //Loop through each and delete
                    int dc = 1;
                    foreach (Guid sg in g)
                    {
                        TryUpdateStatus("Starting the deletion process for SecFiling #" + dc.ToString());

                        //First, delete all Held officer positions that stemmed from this SecFiling
                        TryUpdateStatus("Deleting held officer positions that came from this filing...");
                        await acc.DeleteHeldOfficerPositionsFromFilingAsync(sg);
                        
                        //Now delete the transactions/holdings from the filing, but do it one by one
                        TryUpdateStatus("Getting list of SecurityTransactionHoldings that stemmed from this filing...");
                        Guid[] sthids = await acc.GetSecurityTransactionHoldingIdsFromFilingAsync(sg);
                        TryUpdateStatus(sthids.Length.ToString() + " SecurityTransactionHoldings found.");
                        foreach (Guid tdg in sthids)
                        {
                            TryUpdateStatus("Deleting SecurityTransactionHolding '" + tdg.ToString() + "'...");
                            await acc.DeleteSecurityTransactionHoldingAsync(tdg);
                            TryUpdateStatus("Successfully deleted SecurityTransactionHolding '" + tdg.ToString() + "'");
                        }
                        TryUpdateStatus("Deletion of all SecurityTransactionHoldins that stemmed from this filing complete.");

                        //Now delete the SecFiling itself
                        TryUpdateStatus("Deleting the old SecFiling...");
                        await acc.DeleteSecFilingAsync(sg);

                        //Update status
                        TryUpdateStatus("Cascade deletion of SecFiling #" + dc.ToString() + " complete.");
                        dc = dc + 1;
                    }

                    

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
            TryUpdateStatus("Processing complete!");
            if (ProcessingComplete != null)
            {
                ProcessingComplete.Invoke();
            }
        }

        private async Task ProcessAndUploadFundamentalsFilingAsync(string filing_url, bool overwrite = false)
        {
            //Start processing
            if (ProcessingStarted != null)
            {
                ProcessingStarted.Invoke();
            }

            //Get the details
            TryUpdateStatus("Extracting details from filing...");
            EdgarSearchResult ef = new EdgarSearchResult();
            ef.DocumentsUrl = filing_url;
            EdgarFilingDetails efd = await ef.GetFilingDetailsAsync();

            //Check for duplicates
            TryUpdateStatus("Checking if filing " + efd.AccessionNumberP1.ToString() + "-" + efd.AccessionNumberP2.ToString() + "-" + efd.AccessionNumberP3.ToString() + " has already been processed.");
            Guid[] filingexists = await acc.FindSecFilingAsync(efd.AccessionNumberP1, efd.AccessionNumberP2, efd.AccessionNumberP3);
            if (filingexists.Length > 0)
            {
                TryUpdateStatus("This filing already exists in " + filingexists.Length.ToString() + " record(s)");
                if (overwrite == false)
                {
                    TryUpdateStatus("Since this filing has already been processed, aborting!");
                    return;
                }
                else
                {
                    TryUpdateStatus("SEC Filing has already been processed but will overwrite!");

                    //Delete for each
                    int counter = 1;
                    foreach (Guid g in filingexists)
                    {
                        TryUpdateStatus("Deleting for record #" + counter.ToString());

                        //First, delete the old financial facts
                        TryUpdateStatus("Deleting old financial facts...");
                        await acc.DeleteFinancialFactsFromSecFilingAsync(g);

                        //Second, delete old fact contexts
                        TryUpdateStatus("Deleting old fact contexts...");
                        await acc.DeleteFactContextsFromSecFilingAsync(g);

                        //Finally, delete the SEC filing itself
                        TryUpdateStatus("Deleting old SEC Filing...");
                        await acc.DeleteSecFilingAsync(g);
                    }
                }
            }
            else
            {
                TryUpdateStatus("SEC Filing has not been processed. Continuing!");
            }

            //Prepare the SEC filing
            TryUpdateStatus("Preparing SEC Filing for upload.");
            SecFiling thisfiling = new SecFiling();
            thisfiling.Id = Guid.NewGuid();
            thisfiling.FilingUrl = filing_url;
            thisfiling.AccessionP1 = efd.AccessionNumberP1;
            thisfiling.AccessionP2 = efd.AccessionNumberP2;
            thisfiling.AccessionP3 = efd.AccessionNumberP3;
            if (efd.Form.ToLower().Contains("10-k"))
            {
                thisfiling.FilingType = FilingType.Results10K;
            }
            else if (efd.Form.ToLower().Contains("10-q"))
            {
                thisfiling.FilingType = FilingType.Results10Q;
            }
            else
            {
                throw new Exception("SEC filing form '" + efd.Form + "' not recognized as a fundamentals form.");
            }
            thisfiling.ReportedOn = efd.PeriodOfReport;
            thisfiling.Issuer = efd.EntityCik;
            thisfiling.Owner = null;

            //Upload the SEC filing
            TryUpdateStatus("Uploading SEC filing...");
            await acc.UploadSecFilingAsync(thisfiling);

            //Does the entity exist for this company?
            TryUpdateStatus("Checking if this entity exists in the database...");
            bool EntityExistsAlready = await acc.SecEntityExistsAsync(efd.EntityCik);
            if (EntityExistsAlready == false)
            {
                TryUpdateStatus("Entity does not exist in the database! Preparing and uploading.");
                SecEntity ent = new SecEntity();
                ent.Cik = efd.EntityCik;
                ent.Name = efd.EntityName;
                //ent.TradingSymbol =      I dont have a way of getting the stock symbol from the filing details page for now.
                await acc.UploadSecEntityAsync(ent);
            }
            else
            {
                TryUpdateStatus("Entity " + efd.EntityCik.ToString() + " already exists in the database.");
            }

            //Attempt to download XBRL stream
            Stream s;
            TryUpdateStatus("Downloading XBRL document stream...");
            try
            {
                s = await ef.DownloadXbrlDocumentAsync();
            }
            catch (Exception ex)
            {
                throw new Exception("Failure while downloading XBRL stream. Msg: " + ex.Message);
            }
            TryUpdateStatus("Stream with " + s.Length.ToString("#,##0") + " bytes downloaded.");

            //Parse to Xbrl instance document
            TryUpdateStatus("Parsing into XBRL document...");
            XbrlInstanceDocument doc;
            try
            {
                doc = XbrlInstanceDocument.Create(s);
            }
            catch (Exception ex)
            {
                throw new Exception("Failure while parsing XBRL stream into document. Msg: " + ex.Message);
            }
            TryUpdateStatus("XbrlInstanceDocument generated.");

            //Make into financial statement
            TryUpdateStatus("Converting XBRL instance document into financial statement.");
            FinancialStatement fs;
            try
            {
                fs = doc.CreateFinancialStatement();
            }
            catch (Exception ex)
            {
                throw new Exception("Failure while converting XBRL doc into financial statement. Msg: " + ex.Message);
            }
            TryUpdateStatus("Financial Statement generated.");

            //Process the financial statement
            AletheiaProcessor ap = new AletheiaProcessor();
            FundamentalsProcessingResult fpr;
            TryUpdateStatus("Processing fundamentals from financials...");
            try
            {
                fpr = ap.ProcessFundamentals(fs);
            }
            catch (Exception ex)
            {
                throw new Exception("Failure while processing financials. Msg: " + ex.Message);
            }
            TryUpdateStatus("Fundamentals processed! " + fpr.FactContexts.Length.ToString() + " fact contexts, " + fpr.FinancialFacts.Length.ToString() + " financial facts.");
            
            //Plug in the correct SecFiling ID to all of the fact contexts (this is how the relationship is established)
            foreach (FactContext fc in fpr.FactContexts)
            {
                fc.FromFiling = thisfiling.Id;
            }

            //Upload all
            TryUpdateStatus("Uploading " + fpr.FactContexts.Length.ToString() + " fact contexts and " + fpr.FinancialFacts.Length.ToString() + " financial facts...");
            try
            {
                await acc.UploadFundamentalsProcessingResultAsync(fpr);
            }
            catch (Exception ex)
            {
                throw new Exception("Failure while uploading fundamentals processing result. Msg: " + ex.Message);
            }
            int totaluploaded = fpr.FactContexts.Length + fpr.FinancialFacts.Length;
            TryUpdateStatus("Successfull uploaded " + totaluploaded.ToString() + " fundamentals-related new records.");
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