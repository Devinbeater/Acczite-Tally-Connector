using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Acczite20.Services.Sync;

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

        // Separate HttpClient for large master/voucher exports — Tally can take several minutes
        // to serialize thousands of ledgers or vouchers to XML. The default 30s check timeout
        // would fire mid-export, causing the app to retry while Tally is still busy, leading
        // to memory exhaustion and a Tally crash.
        private readonly HttpClient _exportHttpClient;

        // ── Request Gate ───────────────────────────────────────────────────────────
        // Tally's XML server is single-threaded. One request at a time.
        private static readonly System.Threading.SemaphoreSlim _tallyGate = new(1, 1);
        private static DateTime _lastRequestCompletedAt = DateTime.MinValue;

        // Exponential backoff: delay = BaseDelayMs × 2^failures, capped at MaxDelayMs.
        // This slows the system geometrically as errors accumulate, before the circuit opens.
        //   0 failures → 1500ms
        //   1 failure  → 3000ms
        //   2 failures → 6000ms (capped)
        //   3 failures → circuit opens
        // ── Backoff timing ─────────────────────────────────────────────────────────
        // BaseDelayMs × 2^failures gives the inter-request floor.
        //   0 failures → 4000ms
        //   1 failure  → 8000ms
        //   2 failures → 16000ms
        //   3 failures → circuit opens
        private const int BaseDelayMs       = 4000;   // hard floor between any two requests
        private const int MaxDelayMs        = 30_000; // Tally needs up to 60s to recover after a crash
        private const int StreamExtraCostMs = 2500;   // extra headroom for streaming exports on top of backoff

        // ── Adaptive penalty ───────────────────────────────────────────────────────
        // _extraDelayMs accumulates +1500ms each time a response takes > 8s
        // and decays -500ms each time a response takes < 3s (floor: 0).
        // This gives Tally more breathing room proactively — before the circuit opens.
        private static int _extraDelayMs = 0;
        private const int SlowPenaltyThresholdMs  = 8_000; // > 8s → add 1500ms penalty
        private const int SlowPenaltyAddMs        = 1_500;
        private const int FastRecoveryThresholdMs = 3_000; // < 3s → decay 500ms penalty
        private const int FastRecoveryDecayMs     = 500;

        // ── State-Machine Circuit Breaker ──────────────────────────────────────────
        //
        //   Closed ──(3 weighted failures)──► Open ──(180s)──► HalfOpen
        //                                                           │
        //                                                     1 probe request
        //                                                     ┌────┴────┐
        //                                                  success    failure
        //                                                     │          │
        //                                                  Warming    Open (reset timer)
        //                                                     │
        //                                           3 consecutive successes
        //                                                     │
        //                                                   Closed
        //
        // Failure weights (not all failures are equal):
        //   Empty response             → +1  (Tally struggling but alive)
        //   Timeout (TaskCanceled)     → +2  (Tally busy, might be unresponsive)
        //   Connection refused         → +CircuitBreakerThreshold (open immediately)
        //   Slow response (>5s)        → +1  (pre-throttle before hard failure)
        //
        // All state is static — process-wide, shared by every TallyXmlService instance.
        private enum CircuitState { Closed, Open, HalfOpen, Warming }
        private static CircuitState _circuitState       = CircuitState.Closed;
        private static int  _consecutiveFailures        = 0;
        private static DateTime _circuitOpenedAt        = DateTime.MinValue;
        private static bool _probeInFlight              = false;

        // ── Consecutive-slow cooldown ──────────────────────────────────────────────
        // If the last N responses all took > SlowPenaltyThresholdMs (8s), Tally is in
        // a slow-death spiral. Force a 15s hard pause before the next request.
        // Reset to 0 on any fast response (< FastRecoveryThresholdMs).
        private static int _consecutiveSlowCount = 0;
        private const int CooldownTriggerCount   = 3;
        private const int CooldownDelayMs        = 15_000;
        private static int  _warmingSuccesses           = 0;

        private const int CircuitBreakerThreshold  = 3;    // weighted failure score to open
        private const int CircuitCooldownSeconds   = 180;  // Open → HalfOpen after 3 minutes
        private const int WarmingRequiredSuccesses = 3;    // successes to graduate from Warming → Closed
        private const int SlowResponseThresholdMs  = 3000; // was 5000 — increment failures earlier

        private readonly SyncStateMonitor? _syncMonitor;

        public TallyXmlService(SyncStateMonitor? syncMonitor = null)
        {
            // Quick check client — 30s is plenty for status probes and small requests.
            _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };

            // Export client — with two-pass chunking each request is a fraction of the old
            // single-pass payload. 10 minutes (600s) is required for massive initial Header fetches.
            // Fail-fast on hang: a stalled Tally is worse than a retried smaller chunk.
            _exportHttpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(120) };

            _syncMonitor = syncMonitor;
        }

        private string TallyUrl => $"http://127.0.0.1:{SessionManager.Instance.TallyXmlPort}";

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
                var xml = await SendEnvelopeAsync(@"<ENVELOPE><HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST><TYPE>Data</TYPE><ID>List of Companies</ID></HEADER><BODY><DESC></DESC></BODY></ENVELOPE>");
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

            return knownCollections.ToList();
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

        public async Task<string?> ExportCollectionXmlAsync(string collectionOrReport, DateTimeOffset? fromDate = null, DateTimeOffset? toDate = null, bool isCollection = true, string? idList = null)
        {
            // Master data (Groups, Ledgers, VoucherTypes, Currencies) does NOT need date ranges.
            // Requesting OPENINGBALANCE with a 5-year span forces Tally to recompute balances
            // for every ledger across that entire period — this causes OOM crashes.
            // Only pass dates when explicitly provided (i.e., for voucher-type requests).
            var from = fromDate?.ToString("yyyyMMdd");
            var to = toDate?.ToString("yyyyMMdd");
            
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
                else if (collectionId == "List of Ledgers" || collectionId == "AccziteLedgerHeaders")
                {
                    collectionId = "AccziteLedgerHeaders"; // Use isolated custom collection
                    tdlPart = @"
<TDL>
  <TDLMESSAGE>
    <COLLECTION NAME=""AccziteLedgerHeaders"" ISMODIFY=""No"">
      <TYPE>Ledger</TYPE>
      <FETCH>MASTERID, NAME, PARENT, OPENINGBALANCE</FETCH>
    </COLLECTION>
  </TDLMESSAGE>
</TDL>";
                }
                else if (collectionId == "AccziteLedgerDetails")
                {
                    tdlPart = @"
<TDL>
  <TDLMESSAGE>
    <COLLECTION NAME=""AccziteLedgerDetails"" ISMODIFY=""No"">
      <TYPE>Ledger</TYPE>
      <FETCH>MASTERID, CLOSINGBALANCE, GSTAPPLICABILITY, GSTREGISTRATIONTYPE, PARTYGSTIN, GSTIN, ISBILLWISEON, LEDSTATENAME, INCOMETAXNUMBER, EMAIL, MAILINGNAME, ADDRESS.LIST.*</FETCH>
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

                string idFilterTdl = string.IsNullOrEmpty(idList) ? "" : @"
            <SYSTEM TYPE=""Variable"" NAME=""AccziteIdList"" DATATYPE=""String""/>
            <SYSTEM TYPE=""Formulae"" NAME=""AccziteIdFilter"">
              $$Contains:"",""+##AccziteIdList+"","":"",""+$MASTERID+"",""
            </SYSTEM>";

                string filterSyntax = string.IsNullOrEmpty(idList) ? "" : "\n      <FILTER>AccziteIdFilter</FILTER>";

                // Inject FILTER into the collection definition if it doesn't have one
                if (!string.IsNullOrEmpty(filterSyntax) && tdlPart.Contains("</TYPE>") && !tdlPart.Contains("<FILTER>"))
                {
                    tdlPart = tdlPart.Replace("</TYPE>", "</TYPE>" + filterSyntax);
                }

                // Inject FORMULAE into the TDLMESSAGE if we added a filter
                if (!string.IsNullOrEmpty(idFilterTdl) && tdlPart.Contains("</TDLMESSAGE>"))
                {
                    tdlPart = tdlPart.Replace("</TDLMESSAGE>", idFilterTdl + "\n  </TDLMESSAGE>");
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
        {GetStaticVariablesXml(from, to, idList)}
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

            // Use the 10-minute export client — master collections (especially ledgers) can be large.
            return await SendExportEnvelopeAsync(envelope);
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
            if (IsCircuitOpen())
            {
                _syncMonitor?.AddLog(
                    $"Circuit {_circuitState} ({CircuitCooldownRemaining()}s remaining). Stream blocked.",
                    "WARNING", "CIRCUIT");
                return (null, 0);
            }

            if (_circuitState == CircuitState.HalfOpen && !TryBeginProbe()) return (null, 0);

            await _tallyGate.WaitAsync();
            try
            {
                // Stream requests carry an extra cost (StreamExtraCostMs) on top of adaptive backoff
                // because they are the heaviest operation — a single day-chunk can be 10+ MB.
                var gap   = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                var delay = CurrentDelayMs(isStream: true);
                if (gap < delay) await Task.Delay((int)(delay - gap));

                System.IO.File.WriteAllText("tally_request_stream.xml", envelope);

                var sw = System.Diagnostics.Stopwatch.StartNew();
                var content  = new StringContent(envelope, Encoding.UTF8, "application/xml");
                var response = await _exportHttpClient.PostAsync(TallyUrl, content);
                sw.Stop();
                _lastRequestCompletedAt = DateTime.UtcNow;

                if (response.IsSuccessStatusCode)
                {
                    RecordResponseTime(sw.Elapsed);
                    RecordSuccess();
                    var bytes = await response.Content.ReadAsByteArrayAsync();
                    return (new System.IO.MemoryStream(bytes), bytes.Length);
                }

                RecordFailureWeighted(1);
            }
            catch (Exception ex)
            {
                RecordException(ex);
            }
            finally
            {
                _tallyGate.Release();
            }
            return (null, 0);
        }

        public async Task<TallyXmlStreamLease?> OpenCollectionXmlStreamAsync(
            string collectionOrReport,
            DateTimeOffset? fromDate = null,
            DateTimeOffset? toDate = null,
            bool isCollection = true,
            CancellationToken ct = default,
            string? idList = null)
        {
            var from = (fromDate ?? DateTimeOffset.Now.AddDays(-1)).ToString("yyyyMMdd");
            var to = (toDate ?? DateTimeOffset.Now).ToString("yyyyMMdd");

            string envelope;
            if (isCollection)
            {
                var collectionTdl = GetSingleCollectionTdl(collectionOrReport);
                if (!string.IsNullOrEmpty(idList))
                {
                    // ADD the ID filter alongside the date filter — do NOT replace it.
                    // Multiple <FILTER> entries are AND-ed by Tally, so both constraints apply.
                    // Removing the date filter caused Tally to scan all vouchers from all time
                    // with ALLLEDGERENTRIES.* on each detail batch → OOM crash.
                    collectionTdl = collectionTdl.Replace(
                        "<FILTER>AccziteDateFilter</FILTER>",
                        "<FILTER>AccziteDateFilter</FILTER><FILTER>AccziteIdFilter</FILTER>");
                }

                // $$Contains:MainString:SubString — checks if MainString contains SubString.
                // Wrapping both sides with commas prevents false prefix matches
                // (e.g. "ID1" must not match "ID10,ID11,...").
                // $$InList was wrong here — it expects a named TDL Collection, not a string var.
                string idFilterTdl = string.IsNullOrEmpty(idList) ? "" : @"
            <SYSTEM TYPE=""Variable"" NAME=""AccziteIdList"" DATATYPE=""String""/>
            <SYSTEM TYPE=""Formulae"" NAME=""AccziteIdFilter"">
              $$Contains:"",""+##AccziteIdList+"","":"",""+$$String:$MASTERID+"",""
            </SYSTEM>";

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
        {GetStaticVariablesXml(from, to, idList)}
        <TDL>
          <TDLMESSAGE>
            {collectionTdl}
            {idFilterTdl}
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

            if (IsCircuitOpen())
            {
                _syncMonitor?.AddLog(
                    $"Circuit breaker {_circuitState} ({CircuitCooldownRemaining()}s remaining). Stream export blocked.",
                    "WARNING",
                    "CIRCUIT");
                return null;
            }

            bool isProbe = (_circuitState == CircuitState.HalfOpen);
            if (isProbe && !TryBeginProbe())
            {
                return null;
            }

            bool releaseGate = true;
            HttpResponseMessage? response = null;

            _syncMonitor?.AddLog("Waiting for Tally Gate (stream)...", "DEBUG", "TALLY");
            await _tallyGate.WaitAsync(ct);

            try
            {
                var adaptiveDelay = CurrentDelayMs(isStream: true);
                var msSinceLastRequest = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                if (msSinceLastRequest < adaptiveDelay)
                {
                    await Task.Delay((int)(adaptiveDelay - msSinceLastRequest), ct);
                }

                System.IO.File.WriteAllText("tally_request_stream.xml", envelope);

                using var request = new HttpRequestMessage(HttpMethod.Post, TallyUrl)
                {
                    Content = new StringContent(envelope, Encoding.UTF8, "application/xml")
                };

                response = await _exportHttpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    ct);

                if (!response.IsSuccessStatusCode)
                {
                    RecordFailureWeighted(1);
                    _syncMonitor?.AddLog($"Tally stream request failed with status {(int)response.StatusCode}.", "ERROR", "TALLY");
                    response.Dispose();
                    response = null;
                    return null;
                }

                var stream = await response.Content.ReadAsStreamAsync(ct);
                RecordSuccess();

                releaseGate = false;
                return new TallyXmlStreamLease(
                    stream,
                    response.Content.Headers.ContentLength,
                    async () =>
                    {
                        try
                        {
                            await stream.DisposeAsync();
                        }
                        finally
                        {
                            response.Dispose();
                            _lastRequestCompletedAt = DateTime.UtcNow;
                            _tallyGate.Release();
                            _syncMonitor?.AddLog("Released Tally Gate (stream).", "DEBUG", "TALLY");
                        }
                    });
            }
            catch (Exception ex)
            {
                RecordException(ex);
                return null;
            }
            finally
            {
                if (releaseGate)
                {
                    response?.Dispose();
                    _lastRequestCompletedAt = DateTime.UtcNow;
                    _tallyGate.Release();
                    _syncMonitor?.AddLog("Released Tally Gate (stream).", "DEBUG", "TALLY");
                }
            }
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
              <FETCH>MASTERID, DATE, VOUCHERNUMBER, VOUCHERTYPENAME, ALTERID</FETCH>
            </COLLECTION>",

            "AccziteVoucherLedgers" => @"
            <COLLECTION NAME=""AccziteVoucherLedgers"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ISCANCELLED, ISOPTIONAL, REFERENCE, NARRATION, ALLLEDGERENTRIES.*</FETCH>
            </COLLECTION>",

            // Used as Pass 3 in the min-window 3-pass fallback.
            // Fetches root-level inventory entries — much lighter than ALLLEDGERENTRIES.
            "AccziteVoucherInventory" => @"
            <COLLECTION NAME=""AccziteVoucherInventory"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ISCANCELLED, ISOPTIONAL, ALLINVENTORYENTRIES.*</FETCH>
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

            "AccziteVoucherPipeline" => @"
            <COLLECTION NAME=""AccziteVoucherPipeline"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ALTERID, DATE, VOUCHERNUMBER, REFERENCE, VOUCHERTYPENAME, NARRATION, ISCANCELLED, ISOPTIONAL, ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*, INVENTORYALLOCATIONS.*, TAXCLASSIFICATIONDETAILS.*</FETCH>
            </COLLECTION>",

            // Pass 2 of the two-pass pipeline — ledger + inventory detail only.
            // Excludes scalar header fields so Tally doesn't serialise them twice.
            // ALLLEDGERENTRIES.* includes nested INVENTORYALLOCATIONS and TAXCLASSIFICATIONDETAILS.
            // ALLINVENTORYENTRIES.* covers root-level inventory entries (non-accounting vouchers).
            "AccziteVoucherDetail" => @"
            <COLLECTION NAME=""AccziteVoucherDetail"" ISMODIFY=""No"">
              <TYPE>Voucher</TYPE>
              <FILTER>AccziteDateFilter</FILTER>
              <FETCH>MASTERID, ISCANCELLED, ISOPTIONAL, REFERENCE, NARRATION, ALLLEDGERENTRIES.*, ALLINVENTORYENTRIES.*</FETCH>
            </COLLECTION>",

            "AccziteLedgerHeaders" => @"
            <COLLECTION NAME=""AccziteLedgerHeaders"" ISMODIFY=""No"">
              <TYPE>Ledger</TYPE>
              <FETCH>MASTERID, NAME, PARENT, OPENINGBALANCE, ALTERID</FETCH>
            </COLLECTION>",

            "AccziteLedgerDetails" => @"
            <COLLECTION NAME=""AccziteLedgerDetails"" ISMODIFY=""No"">
              <TYPE>Ledger</TYPE>
              <FETCH>MASTERID, NAME, PARENT, CLOSINGBALANCE, MAILINGNAME, ADDRESS, STATENAME, PINCODE, INCOMETAXNUMBER, EMAIL, ISBILLWISEON, PARTYGSTIN, GSTREGISTRATIONTYPE, GSTAPPLICABILITY</FETCH>
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
            // ExportCollectionXmlAsync injects the "AccziteVoucherTypes" TDL definition,
            // so Tally can resolve the collection. ExportCollectionByIdAsync sends a bare
            // <ID>List of Voucher Types</ID> without TDL — Tally returns a LINEERROR
            // and ContainsElement("VOUCHERTYPE") returns false, making every check look
            // like "no company loaded" even when one is.
            var xml = await ExportCollectionXmlAsync("List of Voucher Types", isCollection: true);
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

        // ── Circuit Breaker state machine ─────────────────────────────────────────

        public static bool IsCircuitOpen()
        {
            if (_circuitState == CircuitState.Closed || _circuitState == CircuitState.Warming)
                return false;

            if (_circuitState == CircuitState.Open)
            {
                if ((DateTime.UtcNow - _circuitOpenedAt).TotalSeconds >= CircuitCooldownSeconds)
                    _circuitState = CircuitState.HalfOpen;
                else
                    return true;
            }

            // HalfOpen: only one probe at a time
            if (_circuitState == CircuitState.HalfOpen)
                return _probeInFlight;

            return false;
        }

        public static int CircuitCooldownRemaining()
        {
            if (_circuitState != CircuitState.Open) return 0;
            var remaining = CircuitCooldownSeconds - (int)(DateTime.UtcNow - _circuitOpenedAt).TotalSeconds;
            return Math.Max(0, remaining);
        }

        /// <summary>
        /// Exponential backoff: delay = BaseDelayMs × 2^failures + _extraDelayMs, capped at MaxDelayMs.
        /// In Warming state a fixed 4s delay gives Tally a soft ramp-up period.
        /// _extraDelayMs is a per-response penalty that accumulates when Tally is slow.
        /// </summary>
        public static int CurrentDelayMs(bool isStream = false)
        {
            int delay;
            if (_circuitState == CircuitState.Warming)
            {
                delay = 4000; // soft ramp-up: fixed 4s (matches new BaseDelayMs) until circuit closes
            }
            else
            {
                var exp = Math.Min(_consecutiveFailures, 4); // cap exponent: 2^4=16 → 64000ms before cap
                delay = (int)(BaseDelayMs * Math.Pow(2, exp));
                delay = Math.Min(delay, MaxDelayMs);
            }
            delay += _extraDelayMs; // adaptive slow-response penalty (0 when Tally is healthy)
            return isStream ? delay + StreamExtraCostMs : delay;
        }

        private void RecordSuccess()
        {
            if (_circuitState == CircuitState.HalfOpen)
            {
                // Probe OK — enter Warming instead of snapping back to full speed
                _circuitState  = CircuitState.Warming;
                _probeInFlight = false;
                _warmingSuccesses = 0;
                System.Threading.Interlocked.Exchange(ref _consecutiveFailures, 1); // start from 1, not 0, for soft ramp
                _syncMonitor?.AddLog("Circuit breaker WARMING — probe succeeded. Ramping up slowly.", "SUCCESS", "CIRCUIT");
                return;
            }

            if (_circuitState == CircuitState.Warming)
            {
                var ws = System.Threading.Interlocked.Increment(ref _warmingSuccesses);
                if (ws >= WarmingRequiredSuccesses)
                {
                    _circuitState = CircuitState.Closed;
                    _warmingSuccesses = 0;
                    System.Threading.Interlocked.Exchange(ref _consecutiveFailures, 0);
                    _syncMonitor?.AddLog("Circuit breaker CLOSED — warm-up complete.", "SUCCESS", "CIRCUIT");
                }
                return;
            }

            System.Threading.Interlocked.Exchange(ref _consecutiveFailures, 0);
        }

        /// <summary>
        /// Records a failure with a given weight. Different failure types carry different weights
        /// so the breaker reacts proportionally to severity.
        /// </summary>
        private void RecordFailureWeighted(int weight)
        {
            // Any failure during recovery re-opens the circuit immediately.
            if (_circuitState == CircuitState.HalfOpen || _circuitState == CircuitState.Warming)
            {
                _circuitOpenedAt  = DateTime.UtcNow;
                _circuitState     = CircuitState.Open;
                _probeInFlight    = false;
                _warmingSuccesses = 0;
                System.Threading.Interlocked.Exchange(ref _consecutiveFailures, CircuitBreakerThreshold);
                _syncMonitor?.AddLog(
                    $"Circuit breaker re-OPENED during recovery. Next probe in {CircuitCooldownSeconds}s.",
                    "ERROR", "CIRCUIT");
                return;
            }

            var failures = System.Threading.Interlocked.Add(ref _consecutiveFailures, weight);
            if (failures >= CircuitBreakerThreshold && _circuitState == CircuitState.Closed)
            {
                _circuitOpenedAt = DateTime.UtcNow;
                _circuitState    = CircuitState.Open;
                _syncMonitor?.AddLog(
                    $"Circuit breaker OPENED (score={failures}). All requests paused for {CircuitCooldownSeconds}s.",
                    "ERROR", "CIRCUIT");
            }
            if (_syncMonitor != null) _syncMonitor.TallyHealth = GetHealthState();
        }

        /// <summary>Classifies an exception and records the appropriate failure weight.</summary>
        private void RecordException(Exception ex)
        {
            if (ex is System.Net.Sockets.SocketException se &&
                (se.SocketErrorCode == System.Net.Sockets.SocketError.ConnectionRefused ||
                 se.SocketErrorCode == System.Net.Sockets.SocketError.HostUnreachable))
            {
                // Connection refused = Tally is dead. Open immediately.
                _syncMonitor?.AddLog($"Tally connection refused — opening circuit immediately.", "ERROR", "CIRCUIT");
                RecordFailureWeighted(CircuitBreakerThreshold);
            }
            else if (ex is TaskCanceledException || ex is TimeoutException ||
                     (ex is HttpRequestException && ex.InnerException is System.Net.Sockets.SocketException))
            {
                // Timeout = Tally is overloaded. Weight 2.
                _syncMonitor?.AddLog($"Tally timeout — backoff increased (weight 2).", "WARNING", "CIRCUIT");
                RecordFailureWeighted(2);
            }
            else
            {
                RecordFailureWeighted(1);
            }
            _syncMonitor?.AddLog($"Tally error: {ex.Message}", "ERROR", "TALLY");
        }

        /// <summary>
        /// Called after a successful response. Manages both the circuit-breaker failure score
        /// and the adaptive _extraDelayMs penalty:
        ///   • response > 8s  → +1500ms penalty + soft failure score increment
        ///   • response > 3s  → soft failure score increment (backoff increase)
        ///   • response &lt; 3s  → decay _extraDelayMs by 500ms
        /// </summary>
        private void RecordResponseTime(TimeSpan elapsed)
        {
            if (elapsed.TotalMilliseconds >= SlowPenaltyThresholdMs && _circuitState == CircuitState.Closed)
            {
                // Very slow (> 8s): escalate per-request penalty AND increment failure score.
                var newExtra = System.Threading.Interlocked.Add(ref _extraDelayMs, SlowPenaltyAddMs);
                System.Threading.Interlocked.Increment(ref _consecutiveFailures);

                // Consecutive-slow cooldown: if N requests in a row all took > 8s, force a
                // hard 15s pause so Tally's GC can recover before the next request fires.
                var slowCount = System.Threading.Interlocked.Increment(ref _consecutiveSlowCount);
                if (slowCount >= CooldownTriggerCount)
                {
                    System.Threading.Interlocked.Exchange(ref _extraDelayMs,
                        Math.Max(_extraDelayMs, CooldownDelayMs));
                    _syncMonitor?.AddLog(
                        $"Tally slow x{slowCount} in a row — forcing {CooldownDelayMs / 1000}s cooldown before next request.",
                        "ERROR", "THROTTLE");
                }
                else
                {
                    _syncMonitor?.AddLog(
                        $"Tally very slow ({elapsed.TotalSeconds:N1}s) — extra delay now {newExtra}ms ({slowCount}/{CooldownTriggerCount} before cooldown).",
                        "WARNING", "THROTTLE");
                }
                return;
            }

            if (elapsed.TotalMilliseconds >= SlowResponseThresholdMs && _circuitState == CircuitState.Closed)
            {
                System.Threading.Interlocked.Increment(ref _consecutiveFailures);
                _syncMonitor?.AddLog(
                    $"Tally slow response ({elapsed.TotalSeconds:N1}s) — backoff increased pre-emptively.",
                    "WARNING", "TALLY");
                return;
            }

            // Fast response — reset slow-streak and decay the extra penalty.
            System.Threading.Interlocked.Exchange(ref _consecutiveSlowCount, 0);
            if (elapsed.TotalMilliseconds < FastRecoveryThresholdMs && _extraDelayMs > 0)
            {
                var newExtra = Math.Max(0, _extraDelayMs - FastRecoveryDecayMs);
                System.Threading.Interlocked.Exchange(ref _extraDelayMs, newExtra);
                if (newExtra == 0)
                    _syncMonitor?.AddLog("Throttle penalties fully recovered — Tally healthy.", "SUCCESS", "THROTTLE");
            }
            if (_syncMonitor != null) _syncMonitor.TallyHealth = GetHealthState();
        }

        /// <summary>
        /// Resets the adaptive throttle penalty and consecutive-slow streak.
        /// Called by VoucherSyncController when entering cooldown mode so the
        /// next request starts with a clean backoff baseline.
        /// </summary>
        public void ResetThrottle()
        {
            System.Threading.Interlocked.Exchange(ref _extraDelayMs, 0);
            System.Threading.Interlocked.Exchange(ref _consecutiveSlowCount, 0);
            _syncMonitor?.AddLog("Throttle reset — adaptive penalty cleared.", "INFO", "THROTTLE");
        }

        /// <summary>
        /// Returns "Stable", "Slow", or "Overloaded" based on current circuit + penalty state.
        /// </summary>
        public string GetHealthState()
        {
            if (_circuitState == CircuitState.Open) return "Overloaded";
            if (_extraDelayMs >= CooldownDelayMs || _consecutiveSlowCount >= CooldownTriggerCount) return "Overloaded";
            if (_extraDelayMs > 0 || _consecutiveFailures > 0) return "Slow";
            return "Stable";
        }

        private static bool TryBeginProbe()
        {
            if (_probeInFlight) return false;
            _probeInFlight = true;
            return true;
        }

        // ── Core send methods ──────────────────────────────────────────────────────

        public async Task<string?> SendEnvelopeAsync(string envelope)
        {
            if (IsCircuitOpen())
            {
                _syncMonitor?.AddLog(
                    $"Circuit {_circuitState} ({CircuitCooldownRemaining()}s remaining). Request blocked.",
                    "WARNING", "CIRCUIT");
                return null;
            }

            if (_circuitState == CircuitState.HalfOpen && !TryBeginProbe()) return null;

            try {
                if (envelope.Length < 5000) System.IO.File.WriteAllText("tally_request.xml", envelope);
            } catch { }

            if (_tallyGate.CurrentCount == 0)
            {
                _syncMonitor?.AddLog("Waiting for Tally Gate (sync)...", "DEBUG", "TALLY");
            }
            await _tallyGate.WaitAsync();
            try
            {
                var gap = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                var delay = CurrentDelayMs();
                if (gap < delay) await Task.Delay((int)(delay - gap));

                var sw = System.Diagnostics.Stopwatch.StartNew();
                var content  = new StringContent(envelope, Encoding.UTF8, "application/xml");
                var response = await _httpClient.PostAsync(TallyUrl, content);
                sw.Stop();
                _lastRequestCompletedAt = DateTime.UtcNow;

                if (!response.IsSuccessStatusCode) { RecordFailureWeighted(1); return null; }

                var rawXml = await response.Content.ReadAsStringAsync();
                RecordResponseTime(sw.Elapsed); // pre-throttle on slowness
                RecordSuccess();
                return SanitizeXml(rawXml);
            }
            catch (Exception ex)
            {
                RecordException(ex);
                return null;
            }
            finally
            {
                _tallyGate.Release();
            }
        }

        /// <summary>
        /// Uses the 10-minute export client. Never use this for connection checks.
        /// </summary>
        private async Task<string?> SendExportEnvelopeAsync(string envelope)
        {
            if (IsCircuitOpen())
            {
                _syncMonitor?.AddLog(
                    $"Circuit {_circuitState} ({CircuitCooldownRemaining()}s remaining). Export blocked.",
                    "WARNING", "CIRCUIT");
                return null;
            }

            if (_circuitState == CircuitState.HalfOpen && !TryBeginProbe()) return null;

            try {
                if (envelope.Length < 5000) System.IO.File.WriteAllText("tally_request.xml", envelope);
            } catch { }

            await _tallyGate.WaitAsync();
            try
            {
                var gap   = (DateTime.UtcNow - _lastRequestCompletedAt).TotalMilliseconds;
                var delay = CurrentDelayMs();
                if (gap < delay) await Task.Delay((int)(delay - gap));

                var sw = System.Diagnostics.Stopwatch.StartNew();
                var content  = new StringContent(envelope, Encoding.UTF8, "application/xml");
                var response = await _exportHttpClient.PostAsync(TallyUrl, content);
                sw.Stop();
                _lastRequestCompletedAt = DateTime.UtcNow;

                if (!response.IsSuccessStatusCode) { RecordFailureWeighted(1); return null; }

                var rawXml = await response.Content.ReadAsStringAsync();
                RecordResponseTime(sw.Elapsed);
                RecordSuccess();
                return SanitizeXml(rawXml);
            }
            catch (Exception ex)
            {
                RecordException(ex);
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

        private string GetStaticVariablesXml(string? fromDate = null, string? toDate = null, string? idList = null)
        {
            var company = SessionManager.Instance.TallyCompanyName?.Trim();
            
            // Critical Binding: Tally returns empty collections if the company context is missing or generic "None"
            var companyVar = !string.IsNullOrEmpty(company) && company != "None" && company != "Default Company"
                ? $"<SVCURRENTCOMPANY>{System.Security.SecurityElement.Escape(company)}</SVCURRENTCOMPANY>" 
                : "";
            
            var dateVars = "";
            if (!string.IsNullOrEmpty(fromDate)) dateVars += $"<SVFROMDATE>{fromDate}</SVFROMDATE>";
            if (!string.IsNullOrEmpty(toDate)) dateVars += $"<SVTODATE>{toDate}</SVTODATE>";

            var idVars = string.IsNullOrEmpty(idList) ? "" : $"<AccziteIdList>{idList}</AccziteIdList>";

            return $@"
      <STATICVARIABLES>
        <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
        {dateVars}
        {companyVar}
        {idVars}
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
