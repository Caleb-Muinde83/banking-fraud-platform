$keep = 9124,28284
Get-CimInstance Win32_Process -Filter "Name='python.exe'" | ForEach-Object {
    if ($keep -notcontains $_.ProcessId) {
        Write-Host "Killing PID $($_.ProcessId): $($_.CommandLine)"
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host "Keeping PID $($_.ProcessId) (simulator): $($_.CommandLine)"
    }
}
Start-Sleep -Seconds 2
Write-Host "--- Remaining python.exe processes ---"
Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Select-Object ProcessId,ParentProcessId,CommandLine | Format-List
