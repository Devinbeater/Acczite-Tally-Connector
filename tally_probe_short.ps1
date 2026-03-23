
$from = (Get-Date).AddDays(-3).ToString("dd-MMM-yyyy")
$to = (Get-Date).ToString("dd-MMM-yyyy")
$envelope = @"
<ENVELOPE>
  <HEADER><TALLYREQUEST>Export Data</TALLYREQUEST></HEADER>
  <BODY>
    <EXPORTDATA>
      <REQUESTDESC>
        <STATICVARIABLES>
          <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
          <SVFromDate>$from</SVFromDate>
          <SVToDate>$to</SVToDate>
        </STATICVARIABLES>
        <REPORTNAME>Day Book</REPORTNAME>
      </REQUESTDESC>
    </EXPORTDATA>
  </BODY>
</ENVELOPE>
"@
try {
  Write-Host "Probing Tally for 3 days ($from to $to)..."
  $r = Invoke-WebRequest -Uri 'http://localhost:9000' -Method Post -Body $envelope -ContentType 'application/xml' -TimeoutSec 10
  $c = $r.Content
  Write-Host "Content Length: $($c.Length)"
  if ($c.Length -gt 1000) { Write-Host "$($c.Substring(0, 1000))..." } else { Write-Host $c }
  if ($c -like '*VOUCHER*') { Write-Host "SUCCESS: VOUCHER found" } else { Write-Host "WARNING: NO VOUCHER found" }
  if ($c -like '*LINEERROR*') { Write-Host "ERROR: Found LINEERROR" }
} catch {
  Write-Host "Error: $($_.Exception.Message)"
}
