# check_port_8000.ps1
#
# Run this before starting the simulator, any time api_requests looks stuck
# at zero despite the container being up. It catches the exact failure mode
# from the July 2026 P0 debugging session: a leftover host-native
# `uvicorn --reload` process binding specifically to 127.0.0.1:8000, which
# Windows prefers over Docker's 0.0.0.0:8000 wildcard listener for any
# client connecting via the literal address "127.0.0.1" -- which is exactly
# what the simulator does by default. When this happens, ALL simulator
# traffic silently lands on the host process instead of bank_api, and the
# container logs stay completely empty with no error anywhere.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File check_port_8000.ps1

$listeners = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue

if (-not $listeners) {
    Write-Host "Nothing is listening on port 8000 yet. Start docker compose and try again." -ForegroundColor Yellow
    exit 0
}

$loopbackOnly = $listeners | Where-Object { $_.LocalAddress -eq "127.0.0.1" }
$wildcard = $listeners | Where-Object { $_.LocalAddress -eq "0.0.0.0" -or $_.LocalAddress -eq "::" }

if ($loopbackOnly) {
    Write-Host "WARNING: something is listening on 127.0.0.1:8000 specifically." -ForegroundColor Red
    Write-Host "This will silently steal simulator traffic away from the bank_api container." -ForegroundColor Red
    foreach ($l in $loopbackOnly) {
        $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$($l.OwningProcess)" -ErrorAction SilentlyContinue
        Write-Host ""
        Write-Host "  PID $($l.OwningProcess): $($proc.Name)"
        Write-Host "  CommandLine: $($proc.CommandLine)"
    }
    Write-Host ""
    Write-Host "Kill it before running the simulator, e.g.:" -ForegroundColor Yellow
    Write-Host "  taskkill //PID <pid> //F   (from Git Bash, note the double slash)" -ForegroundColor Yellow
    exit 1
}

if ($wildcard) {
    Write-Host "OK: only Docker's wildcard listener is bound to port 8000. Safe to proceed." -ForegroundColor Green
    exit 0
}

Write-Host "Unexpected state -- review the listener list manually:" -ForegroundColor Yellow
$listeners | Format-Table LocalAddress, LocalPort, OwningProcess