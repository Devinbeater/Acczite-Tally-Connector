$xml = @"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Data</TYPE>
    <ID>List of Companies</ID>
  </HEADER>
  <BODY>
    <DESC>
    </DESC>
  </BODY>
</ENVELOPE>
"@

Write-Host "Probing Tally for List of Companies at http://127.0.0.1:9000..."
try {
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:9000" -Method Post -Body $xml -ContentType "application/xml" -TimeoutSec 10
    Write-Host "SUCCESS!"
} catch {
    Write-Host "FAILED: $($_.Exception.Message)"
}
