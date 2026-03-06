#Requires -Version 5.1
<#
.SYNOPSIS
    Installs uhoh — ctrl-z for AI agents.
.DESCRIPTION
    Downloads the latest uhoh release from GitHub, installs it,
    and verifies the binary hash against DNS records.
.LINK
    https://uhoh.it/install.ps1
.EXAMPLE
    irm https://uhoh.it/install.ps1 | iex
#>

$ErrorActionPreference = "Stop"

function Main {
    Write-Host "uhoh installer" -ForegroundColor White
    Write-Host ""

    # Already installed?
    $existing = Get-Command "uhoh" -ErrorAction SilentlyContinue
    if ($existing) {
        $currentVersion = try { & uhoh --version 2>&1 } catch { "unknown" }
        Write-Host ("uhoh is already installed at {0}" -f $existing.Source) -ForegroundColor Green
        Write-Host ("  Current version: {0}" -f $currentVersion)
        Write-Host "To update, run: uhoh update"
        return
    }

    # Arch
    $Arch = switch ($env:PROCESSOR_ARCHITECTURE) {
        "AMD64" { "x86_64" }
        "ARM64" { "aarch64" }
        default { throw "Unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
    }
    $AssetName = "uhoh-windows-$Arch"
    Write-Host ("Detected platform: windows/{0} (asset: {1})" -f $Arch, $AssetName)

    # Install dir
    $InstallDir = if ($env:UHOH_INSTALL_DIR) { $env:UHOH_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "uhoh\bin" }
    if (-not (Test-Path $InstallDir)) { New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null }
    $ExePath = Join-Path $InstallDir "uhoh.exe"
    Write-Host ("Install directory: {0}" -f $InstallDir)

    # Release info
    Write-Host "Fetching latest release information..."
    $headers = @{}
    if ($env:GITHUB_TOKEN) { $headers["Authorization"] = "token $env:GITHUB_TOKEN" }
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/fluffypony/uhoh/releases/latest" -Headers $headers
    $asset = $release.assets | Where-Object { $_.name -like "*$AssetName*" } | Select-Object -First 1
    if (-not $asset) { throw "Could not find a release asset matching '$AssetName'" }
    $DownloadUrl = $asset.browser_download_url
    Write-Host ("Download URL: {0}" -f $DownloadUrl)

    # Download
    Write-Host "Downloading..."
    $TempFile = Join-Path $env:TEMP ("uhoh-install-" + [System.IO.Path]::GetRandomFileName() + ".exe")
    Invoke-WebRequest -Uri $DownloadUrl -OutFile $TempFile -UseBasicParsing

    # Verify download hash BEFORE executing the untrusted binary.
    # Use PowerShell's Get-FileHash and DNS TXT record for independent verification.
    Write-Host "Verifying downloaded binary hash..."
    $FileHash = (Get-FileHash -Path $TempFile -Algorithm SHA256).Hash.ToLower()
    Write-Host ("  SHA256: {0}" -f $FileHash)
    try {
        $DnsResult = Resolve-DnsName -Name "sha256.release.uhoh.it" -Type TXT -ErrorAction Stop
        $DnsHash = ($DnsResult | Where-Object { $_.Type -eq "TXT" } | Select-Object -First 1).Strings -join ""
        $DnsHash = $DnsHash.Trim().ToLower()
        if ($DnsHash -and ($FileHash -ne $DnsHash)) {
            Remove-Item $TempFile -ErrorAction SilentlyContinue
            throw "Binary hash does not match DNS record! Expected: $DnsHash, Got: $FileHash"
        } elseif ($DnsHash) {
            Write-Host "  Hash verified against DNS record." -ForegroundColor Green
        } else {
            Write-Host "  DNS record empty; skipping pre-install hash check." -ForegroundColor Yellow
        }
    } catch [System.ComponentModel.Win32Exception] {
        Write-Host "  DNS lookup unavailable; skipping pre-install hash check." -ForegroundColor Yellow
    }

    # Install
    try {
        & uhoh stop | Out-Null
    } catch {
        try {
            taskkill /F /IM uhoh.exe | Out-Null
        } catch {}
    }
    if (Test-Path $ExePath) { Remove-Item $ExePath -Force }
    Move-Item -Path $TempFile -Destination $ExePath -Force
    Write-Host ("Binary installed to {0}" -f $ExePath) -ForegroundColor Green

    # Ensure PATH
    $UserPath = [Environment]::GetEnvironmentVariable("PATH", "User")
    if ($UserPath -notlike ("*{0}*" -f $InstallDir)) {
        [Environment]::SetEnvironmentVariable("PATH", ("{0};{1}" -f $InstallDir, $UserPath), "User")
        $env:PATH = ("{0};{1}" -f $InstallDir, $env:PATH)
        Write-Host ("Added {0} to your user PATH" -f $InstallDir) -ForegroundColor Yellow
    }

    # Verify
    Write-Host "Running post-install verification..."
    try {
        & $ExePath doctor --verify-install
        $exit = $LASTEXITCODE
        if ($exit -eq 0) {
            Write-Host "Binary integrity verified against DNS records." -ForegroundColor Green
        } elseif ($exit -eq 2) {
            Write-Host "WARNING: DNS hash verification failed!" -ForegroundColor Yellow
        } else {
            Write-Host "Could not complete DNS verification." -ForegroundColor Yellow
        }
    } catch {
        Write-Host "Could not run post-install verification." -ForegroundColor Yellow
    }

    Write-Host "uhoh installed successfully!" -ForegroundColor Green
}

Main
