namespace Acczite20.Services.Sync
{
    public static class TallyXmlEnvelopeBuilder
    {
        public static string BuildCollectionRequest(string collectionName, string fromDate, string toDate)
        {
            return $@"
<ENVELOPE>
    <HEADER>
        <TALLYREQUEST>Export Data</TALLYREQUEST>
    </HEADER>
    <BODY>
        <EXPORTDATA>
            <REQUESTDESC>
                <REPORTNAME>{collectionName}</REPORTNAME>
            </REQUESTDESC>
        </EXPORTDATA>
    </BODY>
</ENVELOPE>";
        }

        public static string BuildCompanyList()
        {
            // TallyPrime fix: Use pure TDL Collection for company list as GUI reports can throw TDL errors
            return @"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>AccziteOpenCompanies</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
      <TDL>
        <TDLMESSAGE>
          <COLLECTION NAME=""AccziteOpenCompanies"" ISMODIFY=""No"">
            <TYPE>Company</TYPE>
            <FETCH>NAME, GUID</FETCH>
          </COLLECTION>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>";
        }

        public static string BuildCompanyCollectionRequest()
        {
            return @"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>List of Companies</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
    </DESC>
  </BODY>
</ENVELOPE>";
        }
    }
}
