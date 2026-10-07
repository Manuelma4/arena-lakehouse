param([switch]$RecoverIdlePause)

$ErrorActionPreference = 'Stop'
if (-not $RecoverIdlePause) {
    throw 'Use -RecoverIdlePause only after confirming Docker is idle and its WSL backend is unresponsive. This restarts Docker Desktop and its own distribution.'
}
$arenaDockerRoot = 'C:\Program Files\Docker\Docker'
$arenaDockerExecutable = Join-Path $arenaDockerRoot 'Docker Desktop.exe'
$arenaSettingsRoot = Join-Path ([Environment]::GetFolderPath('ApplicationData')) 'Docker'
$arenaSettingsPath = Join-Path $arenaSettingsRoot 'settings-store.json'
if (-not (Test-Path -LiteralPath $arenaSettingsPath)) { $arenaSettingsPath = Join-Path $arenaSettingsRoot 'settings.json' }
if (-not (Test-Path -LiteralPath $arenaSettingsPath)) { throw 'Docker settings file unavailable; no changes made.' }
$arenaSettings = Get-Content -LiteralPath $arenaSettingsPath -Raw | ConvertFrom-Json
if ($null -eq $arenaSettings.PSObject.Properties['useResourceSaver']) { throw 'Resource Saver setting unavailable; no changes made.' }
$arenaEvidence = Join-Path (Split-Path $PSScriptRoot -Parent) 'data'
New-Item -ItemType Directory -Path $arenaEvidence -Force | Out-Null
$arenaBackup = Join-Path $arenaEvidence ('docker-settings-before-idle-recovery-' + [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss') + '.json')
Copy-Item -LiteralPath $arenaSettingsPath -Destination $arenaBackup
$arenaOriginalMemory = $arenaSettings.memoryMiB
$arenaOriginalCpus = $arenaSettings.cpus

# Stop only processes whose executables resolve inside the installed Docker
# directory. No application, distro, disk image, container volume or data is deleted.
$arenaDockerProcesses = Get-CimInstance Win32_Process -Filter "Name='Docker Desktop.exe' OR Name='com.docker.backend.exe'"
foreach ($arenaProcess in ($arenaDockerProcesses | Sort-Object Name -Descending)) {
    if ($arenaProcess.ExecutablePath -and $arenaProcess.ExecutablePath.StartsWith($arenaDockerRoot + '\', [StringComparison]::OrdinalIgnoreCase)) {
        $arenaCurrent = Get-Process -Id $arenaProcess.ProcessId -ErrorAction SilentlyContinue
        if ($arenaCurrent -and $arenaCurrent.Path -eq $arenaProcess.ExecutablePath) { Stop-Process -Id $arenaCurrent.Id -Force }
    }
}
$arenaSettings.useResourceSaver = $false
$arenaSettingsTemporary = $arenaSettingsPath + '.arena-recovery.tmp'
[IO.File]::WriteAllText($arenaSettingsTemporary, ($arenaSettings | ConvertTo-Json -Depth 100), [Text.UTF8Encoding]::new($false))
Move-Item -LiteralPath $arenaSettingsTemporary -Destination $arenaSettingsPath -Force
$arenaSaved = Get-Content -LiteralPath $arenaSettingsPath -Raw | ConvertFrom-Json
if ($arenaSaved.memoryMiB -ne $arenaOriginalMemory -or $arenaSaved.cpus -ne $arenaOriginalCpus) { throw 'Resource allocations changed unexpectedly.' }

# Terminate only Docker's backend, leaving other WSL distributions alone.
$arenaWsl = Start-Process -FilePath 'wsl.exe' -ArgumentList @('--terminate','docker-desktop') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $arenaEvidence 'docker-idle-wsl-stop.log') -RedirectStandardError (Join-Path $arenaEvidence 'docker-idle-wsl-stop-error.log')
if (-not $arenaWsl.WaitForExit(30000)) {
    Stop-Process -Id $arenaWsl.Id -Force -ErrorAction SilentlyContinue
    throw 'Docker-only WSL termination timed out. Docker remains stopped; no global WSL shutdown or reboot was attempted.'
}
if ($arenaWsl.ExitCode -ne 0) { throw 'Docker-only WSL termination failed; inspect data/docker-idle-wsl-stop-error.log.' }
Start-Process -FilePath $arenaDockerExecutable -WindowStyle Hidden | Out-Null
Write-Output 'Docker restarted with automatic Resource Saver disabled. Verify the Linux Engine and an actual bounded container before claiming recovery.'
Write-Output ('Preserved memoryMiB=' + $arenaOriginalMemory + '; cpus=' + $arenaOriginalCpus)
