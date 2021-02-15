using System;
using SecuritiesExchangeCommission.Edgar;
using System.Collections.Generic;

namespace Aletheia
{
    public class AletheiaProcessor
    {
        public SecurityTransaction[] ProcessForm4(string xml, string sec_accession_num)
        {
            //Create the form4
            StatementOfChangesInBeneficialOwnership form4 = StatementOfChangesInBeneficialOwnership.ParseXml(xml);
            return ProcessForm4(form4, sec_accession_num);
        }
    
        public SecurityTransaction[] ProcessForm4(StatementOfChangesInBeneficialOwnership form4, string sec_accession_num)
        {
            
        }

        #region "Utility Functions"

        
        #endregion
    }
}