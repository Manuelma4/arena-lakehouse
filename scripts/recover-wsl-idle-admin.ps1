param([string]$EvidenceDirectory, [int]$PreviousRecoveryProcessId = 0)

$ErrorActionPreference = 'Stop'
$arenaAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $arenaAdmin) { throw 'Windows Administrator rights are required to restart the stuck WSL service.' }
if (-not $EvidenceDirectory) { $EvidenceDirectory = Join-Path (Split-Path $PSScriptRoot -Parent) 'data' }
$arenaResultPath = Join-Path $EvidenceDirectory 'docker-idle-admin-result.json'
$arenaResult = [ordered]@{ status='starting'; started_at=[DateTime]::UtcNow.ToString('o'); services=@(); other_distributions_closed=$false; memory_allocation_changed=$false; volumes_deleted=$false }
function Save-ArenaResult { [IO.File]::WriteAllText($arenaResultPath, ($arenaResult | ConvertTo-Json -Depth 6), [Text.UTF8Encoding]::new($false)) }
try {
    if ($PreviousRecoveryProcessId -gt 0) {
        $arenaPrevious = Get-CimInstance Win32_Process -Filter "ProcessId=$PreviousRecoveryProcessId"
        if ($arenaPrevious -and $arenaPrevious.Name -eq 'powershell.exe' -and $arenaPrevious.CommandLine -like '*recover-wsl-idle-admin.ps1*') {
            $arenaChildren = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$PreviousRecoveryProcessId AND Name='wsl.exe'")
            Stop-Process -Id $PreviousRecoveryProcessId -Force
            foreach ($arenaChild in $arenaChildren) { Stop-Process -Id $arenaChild.ProcessId -Force -ErrorAction SilentlyContinue }
        }
    }
    Save-ArenaResult
    # The WSL CLI itself can block in this failure. Read registration names
    # directly without waiting on the stuck service; never edit the registry.
    $arenaRegistration = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss'
    $arenaDistros = @(Get-ChildItem -LiteralPath $arenaRegistration | ForEach-Object { (Get-ItemProperty -LiteralPath $_.PSPath).DistributionName } | Where-Object { $_ })
    if ($arenaDistros.Count -eq 0) { throw 'Cannot verify WSL distribution inventory.' }
    if (@($arenaDistros | Where-Object { $_ -notin @('docker-desktop','docker-desktop-data') }).Count -gt 0) {
        throw 'Another WSL distribution is registered; no WSL service restart was attempted.'
    }
    $arenaDockerRoot = 'C:\Program Files\Docker\Docker\'
    foreach ($arenaProcess in (Get-CimInstance Win32_Process -Filter "Name='Docker Desktop.exe' OR Name='com.docker.backend.exe'")) {
        if ($arenaProcess.ExecutablePath -and $arenaProcess.ExecutablePath.StartsWith($arenaDockerRoot, [StringComparison]::OrdinalIgnoreCase)) {
            $arenaCurrent = Get-Process -Id $arenaProcess.ProcessId -ErrorAction SilentlyContinue
            if ($arenaCurrent -and $arenaCurrent.Path -eq $arenaProcess.ExecutablePath) { Stop-Process -Id $arenaCurrent.Id -Force }
        }
    }
    foreach ($arenaName in @('WslService','LxssManager')) {
        $arenaService = Get-Service -Name $arenaName -ErrorAction SilentlyContinue
        if (-not $arenaService) { continue }
        if ($arenaService.Status -ne 'Stopped') {
            $arenaService.Stop()
            $arenaService.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Stopped, [TimeSpan]::FromSeconds(30))
        }
        $arenaService.Start()
        $arenaService.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Running, [TimeSpan]::FromSeconds(30))
        $arenaResult.services += $arenaName
        Save-ArenaResult
    }
    $arenaResult.status = 'wsl_services_restarted'
} catch {
    $arenaResult.status = 'failed'
    $arenaResult.error = $_.Exception.Message
} finally {
    $arenaResult.finished_at = [DateTime]::UtcNow.ToString('o')
    Save-ArenaResult
}
# Docker Desktop is started afterwards by the original non-elevated session.
