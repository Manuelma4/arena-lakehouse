param([switch]$InstallKernel)

$ErrorActionPreference = 'Stop'
$arenaWorkspace = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$arenaInstaller = Join-Path $arenaWorkspace '.arena-tools\wsl_update_x64.msi'
$arenaDockerSettings = Join-Path $env:APPDATA 'Docker\settings.json'
$arenaAdmin = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$arenaElevated = $arenaAdmin.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$arenaMemory = Get-CimInstance Win32_OperatingSystem
$arenaSettings = if (Test-Path -LiteralPath $arenaDockerSettings) {
    Get-Content -LiteralPath $arenaDockerSettings -Raw | ConvertFrom-Json
} else { $null }
[pscustomobject]@{
    Administrator = $arenaElevated
    FreeMemoryMB = [Math]::Round($arenaMemory.FreePhysicalMemory / 1024)
    DockerUsesWSL2 = $arenaSettings.wslEngineEnabled
    DockerConfiguredMemoryMB = $arenaSettings.memoryMiB
    KernelInstallerAvailable = Test-Path -LiteralPath $arenaInstaller
} | Format-List

if (-not $InstallKernel) {
    Write-Output 'Diagnostic only. To install the verified Microsoft kernel, run this script as Administrator with -InstallKernel.'
    exit 0
}
if (-not $arenaElevated) { throw 'Open PowerShell as Administrator and run this script with -InstallKernel.' }
if (-not (Test-Path -LiteralPath $arenaInstaller)) { throw 'The prepared Microsoft WSL kernel installer is missing.' }
$arenaSignature = Get-AuthenticodeSignature -LiteralPath $arenaInstaller
$arenaHash = (Get-FileHash -LiteralPath $arenaInstaller -Algorithm SHA256).Hash
if ($arenaSignature.Status -ne 'Valid' -or $arenaSignature.SignerCertificate.Subject -notlike '*O=Microsoft Corporation*' -or $arenaHash -ne '4D09C776C8D45F70A202281D18E19BE1118F53159B0C217A5274A31CE18525FE') {
    throw 'Microsoft kernel signature or prepared installer checksum did not match.'
}
$arenaLog = Join-Path (Split-Path $PSScriptRoot -Parent) 'data\wsl-kernel-install.log'
New-Item -ItemType Directory -Path (Split-Path $arenaLog -Parent) -Force | Out-Null
$arenaInstall = Start-Process -FilePath 'msiexec.exe' -ArgumentList @('/i', ('"' + $arenaInstaller + '"'), '/qn', '/norestart', '/L*v', ('"' + $arenaLog + '"')) -WindowStyle Hidden -Wait -PassThru
if ($arenaInstall.ExitCode -notin @(0,3010)) { throw "Kernel installation failed with code $($arenaInstall.ExitCode). See data/wsl-kernel-install.log." }
if ($arenaInstall.ExitCode -eq 3010) { Write-Output 'Windows requests a restart. Save your work and restart manually when convenient.' }
Write-Output 'Microsoft WSL kernel installed. No memory allocation or Docker data was changed.'
Write-Output 'In Docker Desktop: Settings > General > Use the WSL 2 based engine > Apply & restart. Then run docker version to verify the Server.'
