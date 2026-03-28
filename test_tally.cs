using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

class Program {
    static async Task Main() {
        var xml = @"<ENVELOPE><HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER><BODY><EXPORTDATA><REQUESTDESC><REPORTNAME>AccziteGroups</REPORTNAME><STATICVARIABLES><SVCURRENTCOMPANY>Vaibhav</SVCURRENTCOMPANY></STATICVARIABLES></REQUESTDESC><TDL><TDLMESSAGE><COLLECTION NAME=""AccziteGroups"" ISMODIFY=""No""><TYPE>Group</TYPE><FETCH>MASTERID, NAME, PARENT, NATUREOFGROUP, ISPRIMARY, AFFECTSGROSSPROFIT</FETCH></COLLECTION></TDLMESSAGE></TDL></EXPORTDATA></BODY></ENVELOPE>";
        using var client = new HttpClient();
        var response = await client.PostAsync("http://localhost:9000/", new StringContent(xml, Encoding.UTF8, "text/xml"));
        var result = await response.Content.ReadAsStringAsync();
        Console.WriteLine(result.Substring(0, Math.Min(result.Length, 1500)));
    }
}
