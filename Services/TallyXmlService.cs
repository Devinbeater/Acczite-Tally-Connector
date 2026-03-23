using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Acczite20.Services
{
    /// <summary>
    /// Tally connection states for enterprise UI clarity.
    /// </summary>
    public enum TallyConnectionStatus
    {
        NotRunning,           // 🔴 Both XML + ODBC failed
        RunningNoCompany,     // 🟡 Responds but no company open
        RunningWithCompany,   // 🟢 Responds and company loaded
        Error                 // 🟠 Request failed with error
    }

    /// <summary>
    /// Enterprise Tally connector with dual-mode detection (XML + ODBC failover),
    /// timeout protection, and optimized field loading.
    /// </summary>
    public class TallyXmlService
    {
        private readonly HttpClient _httpClient;

        // Tally's XML server is single-threaded. Concurrent or rapid-fire requests
        // cause it to queue up responses, run out of memory, and crash.
        // This gate ensures: (1) only one request is in-flight at a time,
        // (2) a minimum inter-request gap is enforced so Tally can breathe.
        private static readonly System.Threading.SemaphoreSlim _tallyGate = new(1, 1);
        private static DateTime _lastRequestCompletedAt = DateTime.MinValue;
        private const int MinInterRequestDelayMs = 800; // Tally needs ~500ms to clean up between requests
        private readonly SyncStateMonitor? _syncMonitor;

        public TallyXmlService(SyncStateMonitor? syncMonitor = null)
        {
            _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
            _syncMonitor = syncMonitor;
        }

        private string TallyUrl => $"http://localhost:{SessionManager.Instance.TallyXmlPort}";

        // ════════════════════════════════════════════════════
        //  1. DUAL-MODE DETECTION (XML → ODBC fallover)
        // ════════════════════════════════════════════════════
        public async Task<TallyConnectionStatus> DetectTallyStatusAsync()
        {
            _syncMonitor?.AddLog("Checking Tally XML connectivity...", "DEBUG", "TALLY");
            if (await IsTallyRunningAsync())
            {
                _syncMonitor?.AddLog("Tally XML responded. Verifying company status...", "DEBUG", "TALLY");
                if (await HasLoadedCompanyAsync())
                {
                    _syncMonitor?.AddLog("Tally is running with an open company.", "SUCCESS", "TALLY");
                    return TallyConnectionStatus.RunningWithCompany;
                }
                _syncMonitor?.AddLog("Tally is running but NO company is found.", "WARNING", "TALLY");
                return TallyConnectionStatus.RunningNoCompany;
            }

            _syncMonitor?.AddLog("Tally XML disconnected. Attempting ODBC fallback...", "DEBUG", "TALLY");
            
            try
            {
                string odbcConnStr = $"Driver={{Tally ODBC Driver64}};Server=localhost;Port={SessionManager.Instance.TallyOdbcPort};Connect Timeout=3;";
                using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(3));
                using var conn = new System.Data.Odbc.OdbcConnection(odbcConnStr);
                await conn.OpenAsync(cts.Token);
                _syncMonitor?.AddLog("Tally ODBC (64-bit) responded.", "SUCCESS", "TALLY");
                return TallyConnectionStatus.RunningNoCompany;
            }
            catch { }
            try
            {
                string odbcConnStr32 = $"Driver={{Tally ODBC Driver}};Server=localhost;Port={SessionManager.Instance.TallyOdbcPort};Connect Timeout=3;";
                using var cts = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(3));
                using var conn = new System.Data.Odbc.OdbcConnection(odbcConnStr32);
                await conn.OpenAsync(cts.Token);
                _syncMonitor?.AddLog("Tally ODBC (32-bit) responded.", "SUCCESS", "TALLY");
                return TallyConnectionStatus.RunningNoCompany; 
            }
            catch { }

            _syncMonitor?.AddLog("Tally not responding on XML or ODBC ports.", "ERROR", "TALLY");
            return TallyConnectionStatus.NotRunning;
        }

        public async Task<bool> IsTallyRunningAsync()
        {
            try
            {
                var xml = await SendEnvelopeAsync(BuildEnvelope("List of Companies"));
                return !string.IsNullOrWhiteSpace(xml);
            }
            catch { return false; }
        }

        public async Task<string> GetCurrentCompanyNameAsync()
        {
            // TallyPrime fix: Use exact TDL Collection instead of the 'Company' report which can throw ""No PARTS"" error
            var envelope = @"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>AccziteActiveCompany</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
      </STATICVARIABLES>
      <TDL>
        <TDLMESSAGE>
          <COLLECTION NAME=""AccziteActiveCompany"" ISMODIFY=""No"">
            <TYPE>Company</TYPE>
            <FETCH>NAME</FETCH>
          </COLLECTION>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>";
            try
            {
                var response = await SendEnvelopeAsync(envelope);
                var company = ExtractCompanyName(response);
                if (!IsUnresolvedCompany(company))
                {
                    return company!;
                }

                var probeXml = await ExportCollectionByIdAsync("List of Voucher Types");
                company = ExtractCompanyName(probeXml);
                if (!IsUnresolvedCompany(company))
                {
                    return company!;
                }

                var masterXml = await ExportCollectionXmlAsync("List of Groups", isCollection: true);
                company = ExtractCompanyName(masterXml);
                return IsUnresolvedCompany(company) ? "None" : company!;
            }
            catch { return "None"; }
        }

        public async Task<decimal> GetTallyTrialBalanceTotalAsync()
        {
            // We fetch the 'Summary' which is the base of Trial Balance but much lighter to parse.
            // Specifically, we want the Total Opening Balance of all ledgers to verify masters.
            string envelope = $@"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        {GetStaticVariablesXml()}
        <REPORTNAME>Trial Balance</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";

            try
            {
                var response = await SendEnvelopeAsync(envelope);
                if (string.IsNullOrEmpty(response)) return 0;

                // Tally XML reports are often deep. We look for the DEBIT/CREDIT grand totals.
                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var reader = System.Xml.XmlReader.Create(new System.IO.StringReader(response), settings);
                var doc = XDocument.Load(reader);

                // Trial Balance Totals are usually at the end of the report in <DSPTOTALS> or similar.
                // We'll search for the numeric value associated with opening/closing sums.
                var totalNode = doc.Descendants().LastOrDefault(x => x.Name.LocalName.Equals("DSPTOTAL", StringComparison.OrdinalIgnoreCase));
                if (totalNode != null && decimal.TryParse(totalNode.Value.Replace(",", ""), out var val)) return Math.Abs(val);

                // Fallback: look for any 'OpeningBalance' style tag in summary
                var summaryNode = doc.Descendants().FirstOrDefault(x => x.Name.LocalName.Contains("OPENING") && x.Name.LocalName.Contains("TOTAL"));
                if (summaryNode != null && decimal.TryParse(summaryNode.Value.Replace(",", ""), out var sVal)) return Math.Abs(sVal);
            }
            catch { }
            return 0;
        }

        public async Task<List<string>> GetTallyCompaniesAsync()
        {
            var envelope = @"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        <STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
        <REPORTNAME>List of Companies</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";
            
            var response = await SendEnvelopeAsync(envelope);
            var companies = new List<string>();

            if (!string.IsNullOrWhiteSpace(response))
            {
                try
                {
                    // Use CheckCharacters = false to ignore 0x04 and other illegal XML chars
                    var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                    using var reader = System.Xml.XmlReader.Create(new System.IO.StringReader(response), settings);
                    var doc = XDocument.Load(reader);
                    
                    var compNodes = doc.Descendants().Where(x => x.Name.LocalName.Equals("COMPANY", StringComparison.OrdinalIgnoreCase)).ToList();
                    
                    if (compNodes.Count > 0)
                    {
                        foreach (var node in compNodes)
                        {
                            var name = node.Elements().FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))?.Value 
                                    ?? node.Attributes().FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))?.Value;
                            if (!string.IsNullOrEmpty(name)) companies.Add(name);
                        }
                    }
                    
                    if (companies.Count == 0)
                    {
                        companies = doc.Descendants().Where(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                                       .Select(x => x.Value).Distinct().ToList();
                    }
                }
                catch { }
            }

            var current = await GetCurrentCompanyNameAsync();
            if (!IsUnresolvedCompany(current))
            {
                companies.RemoveAll(x => string.Equals(x?.Trim(), current, StringComparison.OrdinalIgnoreCase));
                companies.Insert(0, current);
            }

            return companies
                .Where(x => !IsUnresolvedCompany(x))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        public async Task<List<string>> GetTallyCollectionsAsync()
        {
            var knownCollections = new[]
            {
                "List of Ledgers", "List of Groups", "List of Voucher Types", "List of Stock Items",
                "List of Stock Groups", "List of Cost Centres", "List of Cost Categories", "List of Currencies",
                "List of Budgets", "List of Units", "List of Godowns", "List of Payroll Categories",
                "List of Payroll Cost Centres", "List of Attendance Types", "List of Employees", "Voucher"
            };

            var available = new List<string>();
            foreach (var col in knownCollections)
            {
                try
                {
                    var envelope = BuildEnvelope(col);
                    var response = await SendEnvelopeAsync(envelope);
                    if (!string.IsNullOrWhiteSpace(response) && response.Contains("<"))
                    {
                        available.Add(col);
                    }
                    await Task.Delay(300); // Throttling: prevent DDOS-ing Tally
                }
                catch { }
            }

            return available;
        }

        public async Task<List<string>> GetCollectionFieldsAsync(string reportName)
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var envelope = $@"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        {GetStaticVariablesXml(today, today)}
        <REPORTNAME>{reportName}</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";
            try
            {
                var response = await SendEnvelopeAsync(envelope);
                if (string.IsNullOrWhiteSpace(response)) return new List<string>();

                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var reader = System.Xml.XmlReader.Create(new System.IO.StringReader(response), settings);
                var doc = XDocument.Load(reader);
                var excludeNodes = new HashSet<string> { "ENVELOPE", "HEADER", "BODY", "EXPORTDATA", "REQUESTDESC", "REPORTNAME", "TALLYREQUEST", "STATICVARIABLES", "SVEXPORTFORMAT", "SVFROMDATE", "SVTODATE", "DESC", "LINENO", "SVCURRENTCOMPANY", "SVCURRENTCOMPANY" };
                return doc.Descendants().Where(e => !e.HasElements && e.Parent != null).Select(e => e.Name.LocalName).Where(n => !excludeNodes.Contains(n, StringComparer.OrdinalIgnoreCase)).Distinct().OrderBy(n => n).ToList();
            }
            catch { return new List<string>(); }
        }

        public async Task<string?> ExportCollectionXmlAsync(string collectionOrReport, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null, bool isCollection = true)
        {
            var from = (fromDate ?? DateTimeOffset.Now.AddYears(-5)).ToString("yyyyMMdd");
            var to = (toDate ?? DateTimeOffset.Now).ToString("yyyyMMdd");
            
            string envelope;
            if (isCollection)
            {
                // Enhanced Enterprise-Grade TDL Injection for Masters
                string tdlPart = "";
                string collectionId = collectionOrReport;

                // Handle common Master mapping to ensure they never return 0 due to Tally GUI inconsistencies
                if (collectionId == "List of Groups")
                {
                    collectionId = "AccziteGroups"; // Use isolated custom collection
                    tdlPart = @"
<TDL>
  <TDLMESSAGE>
    <COLLECTION NAME=""AccziteGroups"" ISMODIFY=""No"">
      <TYPE>Group</TYPE>
      <FETCH>MASTERID, NAME, PARENT, NATUREOFGROUP, ISPRIMARY, AFFECTSGROSSPROFIT</FETCH>
    </COLLECTION>
  </TDLMESSAGE>
</TDL>";
                }
                else if (collectionId == "List of Ledgers")
                {
                    collectionId = "AccziteLedgers"; // Use isolated custom collection
                    tdlPart = @"
<TDL>
  <TDLMESSAGE>
    <COLLECTION NAME=""AccziteLedgers"" ISMODIFY=""No"">
      <TYPE>Ledger</TYPE>
      <FETCH>MASTERID, NAME, PARENT, OPENINGBALANCE, CLOSINGBALANCE, GSTAPPLICABILITY, GSTREGISTRATIONTYPE, PARTYGSTIN, GSTIN, ISBILLWISEON, LEDSTATENAME, INCOMETAXNUMBER, EMAIL, MAILINGNAME</FETCH>
    </COLLECTION>
  </TDLMESSAGE>
</TDL>";
                }
                else if (collectionId == "List of Voucher Types")
                {
                    collectionId = "AccziteVoucherTypes";
                    tdlPart = @"
<TDL>
  <TDLMESSAGE>
    <COLLECTION NAME=""AccziteVoucherTypes"" ISMODIFY=""No"">
      <TYPE>VoucherType</TYPE>
      <FETCH>NAME, PARENT</FETCH>
    </COLLECTION>
  </TDLMESSAGE>
</TDL>";
                }

                envelope = $@"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>{collectionId}</ID>
  </HEADER>
  <BODY>
    <DESC>
        {GetStaticVariablesXml(from, to)}
        {tdlPart}
    </DESC>
  </BODY>
</ENVELOPE>";
            }
            else
            {
                envelope = $@"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        {GetStaticVariablesXml(from, to)}
        <REPORTNAME>{collectionOrReport}</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";
            }

            // Persistence for Debug
            try { System.IO.File.WriteAllText("tally_master_request.xml", envelope); } catch {}

            return await SendEnvelopeAsync(envelope);
        }

        public async Task<(System.IO.Stream? Stream, long Size)> ExportCollectionXmlStreamAsync(string collectionOrReport, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null, bool isCollection = true)
        {
            var from = (fromDate ?? DateTimeOffset.Now.AddDays(-1)).ToString("yyyyMMdd");
            var to   = (toDate   ?? DateTimeOffset.Now).ToString("yyyyMMdd");

            string envelope;
            if (isCollection)
            {
                // Each pass now defines ONLY the specific collection it needs.
                // Previously all 4 collection definitions were embedded in every request,
                // forcing Tally to compile 4× the work per pass. This reduced Tally to
                // a single targeted TDL definition per HTTP call.
                var collectionTdl = GetSingleCollectionTdl(collectionOrReport);
                envelope = $@"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>{collectionOrReport}</ID>
  </HEADER>
  <BODY>
    <DESC>
        {GetStaticVariablesXml(from, to)}
        <TDL>
          <TDLMESSAGE>
            {collectionTdl}
            <SYSTEM TYPE=""Formulae"" NAME=""AccziteDateFilter"">
              $$IsBetween:$Date:##SVFROMDATE:##SVTODATE
            </SYSTEM>
          </TDLMESSAGE>
        </TDL>
    </DESC>
  </BODY>
</ENVELOPE>";
            }
            else
            {
                envelope = $@"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        {GetStaticVariablesXml(from, to)}
        <REPORTNAME>{collectionOrReport}</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";
            }
            await _tallyGate.WaitAsync();
            try
            {
                var msSinceLastRequest = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                if (msSinceLastRequest < MinInterRequestDelayMs)
                    await Task.Delay((int)(MinInterRequestDelayMs - msSinceLastRequest));

                System.IO.File.WriteAllText("tally_request_stream.xml", envelope);

                var content = new StringContent(envelope, Encoding.UTF8, "application/xml");
                var response = await _httpClient.PostAsync(TallyUrl, content);
                _lastRequestCompletedAt = DateTime.UtcNow;

                if (response.IsSuccessStatusCode)
                {
                    var bytes = await response.Content.ReadAsByteArrayAsync();
                    return (new System.IO.MemoryStream(bytes), bytes.Length);
                }
            }
            catch (Exception ex)
            {
                _syncMonitor?.AddLog($"Tally Stream Failure: {ex.Message}", "ERROR", "TALLY");
            }
            finally
            {
                _tallyGate.Release();
            }
            return (null, 0);
        }

        /// <summary>
        /// Returns the TDL COLLECTION definition for only the requested collection name.
        /// Keeping definitions isolated prevents Tally from compiling unused collections
        /// on every request, which was the primary cause of Tally XML server crashes.
        /// </summary>
        private static string GetSingleCollectionTdl(string collectionName) => collectionName switch
        {
            "AccziteVoucherHeaders" => @"
            <COLLECTION NAME=""AccziteVoucherHeaders"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ALTERID, DATE, VOUCHERNUMBER, VOUCHERTYPENAME, NARRATION, ISCANCELLED, ISOPTIONAL</FETCH>
            </COLLECTION>",

            "AccziteVoucherLedgers" => @"
            <COLLECTION NAME=""AccziteVoucherLedgers"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ALLLEDGERENTRIES.*</FETCH>
            </COLLECTION>",

            "AccziteVoucherInventory" => @"
            <COLLECTION NAME=""AccziteVoucherInventory"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, INVENTORYALLOCATIONS.*</FETCH>
            </COLLECTION>",

            "AccziteVoucherGST" => @"
            <COLLECTION NAME=""AccziteVoucherGST"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, TAXCLASSIFICATIONDETAILS.*</FETCH>
            </COLLECTION>",

            "AccziteVoucherDiscoveryCollection" => @"
            <COLLECTION NAME=""AccziteVoucherDiscoveryCollection"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ALTERID, DATE, VOUCHERNUMBER</FETCH>
            </COLLECTION>",

            // Fallback: request the named collection without a FETCH restriction
            _ => $@"
            <COLLECTION NAME=""{collectionName}"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
            </COLLECTION>"
        };

        private async Task<bool> HasLoadedCompanyAsync()
        {
            var xml = await ExportCollectionByIdAsync("List of Voucher Types");
            return ContainsElement(xml, "VOUCHERTYPE");
        }

        private async Task<string?> ExportCollectionByIdAsync(string collectionId)
        {
            var envelope = $@"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>{collectionId}</ID>
  </HEADER>
  <BODY>
    <DESC>
      {GetStaticVariablesXml()}
    </DESC>
  </BODY>
</ENVELOPE>";

            return await SendEnvelopeAsync(envelope);
        }

        public async Task<string?> SendEnvelopeAsync(string envelope)
        {
            try { 
                if (envelope.Length < 5000)
                    System.IO.File.WriteAllText("tally_request.xml", envelope); 
            } catch { }

            // Serialize all Tally requests and enforce a minimum inter-request gap
            // to prevent Tally's XML server from being overwhelmed.
            await _tallyGate.WaitAsync();
            try
            {
                var msSinceLastRequest = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                if (msSinceLastRequest < MinInterRequestDelayMs)
                    await Task.Delay((int)(MinInterRequestDelayMs - msSinceLastRequest));

                var content = new StringContent(envelope, Encoding.UTF8, "application/xml");
                var response = await _httpClient.PostAsync(TallyUrl, content);
                _lastRequestCompletedAt = DateTime.UtcNow;

                if (!response.IsSuccessStatusCode) return null;

                var rawXml = await response.Content.ReadAsStringAsync();
                return SanitizeXml(rawXml);
            }
            catch (Exception ex)
            {
                _syncMonitor?.AddLog($"Tally Request Failure: {ex.Message}", "ERROR", "TALLY");
                return null;
            }
            finally
            {
                _tallyGate.Release();
            }
        }

        private static string SanitizeXml(string? xml)
        {
            if (string.IsNullOrEmpty(xml)) return string.Empty;
            
            // Fast scan for illegal characters
            bool hasBadChar = false;
            foreach (char c in xml)
            {
                if (!((c >= 0x20 && c <= 0xD7FF) || 
                      c == 0x09 || c == 0x0A || c == 0x0D || 
                      (c >= 0xE000 && c <= 0xFFFD)))
                {
                    hasBadChar = true;
                    break;
                }
            }

            if (!hasBadChar) return xml;

            // Efficient StringBuilder sanitization for large strings
            var sb = new StringBuilder(xml.Length);
            foreach (char c in xml)
            {
                if ((c >= 0x20 && c <= 0xD7FF) || 
                    c == 0x09 || c == 0x0A || c == 0x0D || 
                    (c >= 0xE000 && c <= 0xFFFD))
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }

        private string GetStaticVariablesXml(string? fromDate = null, string? toDate = null)
        {
            var company = SessionManager.Instance.TallyCompanyName?.Trim();
            
            // Critical Binding: Tally returns empty collections if the company context is missing or generic "None"
            var companyVar = !string.IsNullOrEmpty(company) && company != "None" && company != "Default Company"
                ? $"<SVCURRENTCOMPANY>{System.Security.SecurityElement.Escape(company)}</SVCURRENTCOMPANY>" 
                : "";
            
            var dateVars = "";
            if (!string.IsNullOrEmpty(fromDate)) dateVars += $"<SVFROMDATE>{fromDate}</SVFROMDATE>";
            if (!string.IsNullOrEmpty(toDate)) dateVars += $"<SVTODATE>{toDate}</SVTODATE>";

            return $@"
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
        <SVEXPORTLIMIT>200</SVEXPORTLIMIT>
        {dateVars}
        {companyVar}
      </STATICVARIABLES>";
        }

        private string BuildEnvelope(string reportName)
        {
            return $@"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        {GetStaticVariablesXml()}
        <REPORTNAME>{reportName}</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>";
        }

        private static bool ContainsElement(string? xml, string elementName)
        {
            if (string.IsNullOrWhiteSpace(xml) || HasLineError(xml)) return false;
            return xml.IndexOf($"<{elementName}", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static bool HasLineError(string? xml)
        {
            return !string.IsNullOrWhiteSpace(xml) && xml.IndexOf("<LINEERROR>", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private static string? ExtractCompanyName(string? xml)
        {
            if (string.IsNullOrWhiteSpace(xml) || HasLineError(xml))
            {
                return null;
            }

            try
            {
                // Loose parsing allows illegal 0x04 chars from Tally to pass without crashing
                var settings = new System.Xml.XmlReaderSettings { CheckCharacters = false };
                using var stringReader = new System.IO.StringReader(xml!);
                using var reader = System.Xml.XmlReader.Create(stringReader, settings);
                var doc = XDocument.Load(reader);
                var currentCompany = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("SVCURRENTCOMPANY", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim();

                if (!IsUnresolvedCompany(currentCompany))
                {
                    return currentCompany;
                }

                var companyTag = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("COMPANYNAME", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim();

                if (!IsUnresolvedCompany(companyTag))
                {
                    return companyTag;
                }

                var companyNode = doc.Descendants()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("COMPANY", StringComparison.OrdinalIgnoreCase));

                var companyName = companyNode?.Elements()
                    .FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                    ?.Value
                    ?.Trim()
                    ?? companyNode?.Attributes()
                        .FirstOrDefault(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                        ?.Value
                        ?.Trim();

                if (!IsUnresolvedCompany(companyName))
                {
                    return companyName;
                }

                return doc.Descendants()
                    .Where(x => x.Name.LocalName.Equals("NAME", StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Value?.Trim())
                    .FirstOrDefault(x => !IsUnresolvedCompany(x));
            }
            catch
            {
                return null;
            }
        }

        private static bool IsUnresolvedCompany(string? companyName)
        {
            return string.IsNullOrWhiteSpace(companyName)
                || string.Equals(companyName, "None", StringComparison.OrdinalIgnoreCase)
                || string.Equals(companyName, "Default Company", StringComparison.OrdinalIgnoreCase);
        }
    }
}
