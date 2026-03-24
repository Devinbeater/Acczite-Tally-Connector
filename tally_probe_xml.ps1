$xml = @"
<ENVELOPE>
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>Export</TALLYREQUEST>
    <TYPE>Collection</TYPE>
    <ID>List of Companies</ID>
  </HEADER>
  <BODY>
    <DESC></DESC>
  </BODY>
</ENVELOPE>
"@

try {
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:9000" -Method Post -Body $xml -ContentType "application/xml" -TimeoutSec 10
    Write-Host "SUCCESS: $($response | Out-String)"
} catch {
    Write-Host "FAILED: $($_.Exception.Message)"
}
