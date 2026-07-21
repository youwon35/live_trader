$ErrorActionPreference = "Stop"
$taskName = "TradingSystem-LiveTrader-Monitor"
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
  Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
  Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}
Write-Output "Removed: $taskName"
