# p2p installer for Windows. Usage:
#   irm https://raw.githubusercontent.com/jdp5949/p2p-messaging/main/install.ps1 | iex
$ErrorActionPreference = "Stop"
$repo = "jdp5949/p2p-messaging"
$asset = "p2p-windows-amd64.exe"
$url = "https://github.com/$repo/releases/latest/download/$asset"

$dir = Join-Path $env:LOCALAPPDATA "p2p"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$dest = Join-Path $dir "p2p.exe"

Write-Host "Downloading $asset…"
Invoke-WebRequest -Uri $url -OutFile $dest

# Add install dir to the user PATH if missing.
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$dir*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$dir", "User")
    Write-Host "Added $dir to your user PATH."
}

Write-Host "installed p2p. Open a NEW terminal, then run: p2p send"
