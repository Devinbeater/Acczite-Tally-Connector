$xml = Get-Content "c:\Users\OM WAGH\source\repos\Acczite20\tally_master_request.xml" -Raw

Write-Host "Probing Tally at http://127.0.0.1:9000 with tally_master_request.xml..."
try {
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:9000" -Method Post -Body $xml -ContentType "application/xml" -TimeoutSec 15
    Write-Host "SUCCESS!"
    $responseString = $response | Out-String
    if ($responseString.Length -gt 1000) {
        Write-Host "Response length: $($responseString.Length) characters. Showing first 500:"
        Write-Host ($responseString.Substring(0, 500))
    } else {
        Write-Host "Response: $responseString"
    }
} catch {
    Write-Host "FAILED: $($_.Exception.Message)"
}
