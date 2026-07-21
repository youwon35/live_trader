param(
  [string]$Executable = (Join-Path (Split-Path -Parent $PSScriptRoot) "release\LiveTrader.exe")
)

$ErrorActionPreference = "Stop"
$resolved = (Resolve-Path -LiteralPath $Executable).Path
$taskName = "TradingSystem-LiveTrader-Monitor"
$action = New-ScheduledTaskAction -Execute $resolved -Argument "--daemon --profiles stock,crypto --mode MONITOR --poll-seconds 30"
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet -RestartCount 99 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description "Live Trader market/execution MONITOR service" -Force | Out-Null
Start-ScheduledTask -TaskName $taskName
Write-Output "Installed and started: $taskName"
