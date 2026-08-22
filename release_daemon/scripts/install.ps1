<#
.SYNOPSIS
    HITL daemon installer for Windows.

.DESCRIPTION
    Obtained by signing in to the website and copying the command, which carries
    a presigned artifact URL and its expected hash. Both expire, so an expired
    link is reported as such rather than as a generic web error.

    irm "<script-url>" | iex; Install-HitlDaemon -Url "<artifact-url>" -Sha256 "<hash>"
#>

$ErrorActionPreference = 'Stop'

$EXIT_UNSUPPORTED = 3
$EXIT_EXPIRED = 4
$EXIT_CHECKSUM = 5
$EXIT_DOWNLOAD = 6

function Install-HitlDaemon {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][string]$Sha256,
        [string]$InstallDir = (Join-Path $env:LOCALAPPDATA 'Programs\hitl-daemon')
    )

    if (-not [Environment]::Is64BitOperatingSystem) {
        Write-Error 'No daemon is published for 32-bit Windows. Supported: Windows x86_64.'
        exit $EXIT_UNSUPPORTED
    }

    # Everything lands in a temp directory until every check has passed, so a
    # failure leaves no partial installation behind.
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
    New-Item -ItemType Directory -Path $tmp -Force | Out-Null
    $downloaded = Join-Path $tmp 'hitl-daemon.exe'

    try {
        Write-Host 'Downloading daemon...'
        try {
            Invoke-WebRequest -Uri $Url -OutFile $downloaded -UseBasicParsing
        }
        catch {
            $response = $_.Exception.Response
            $status = if ($response) { [int]$response.StatusCode } else { 0 }
            $body = ''
            if ($response) {
                try {
                    $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
                    $body = $reader.ReadToEnd()
                }
                catch { $body = '' }
            }
            # S3 answers an expired presigned URL with 403 and "Request has
            # expired" in the body; reporting that raw reads as a permissions
            # problem the user cannot act on.
            if ($status -eq 403 -and $body -match 'Request has expired|ExpiredToken') {
                Write-Error 'This install link has expired. Sign in to the website and copy a fresh command.'
                exit $EXIT_EXPIRED
            }
            Write-Error "Download failed (HTTP $status)."
            exit $EXIT_DOWNLOAD
        }

        $actual = (Get-FileHash -Path $downloaded -Algorithm SHA256).Hash.ToLower()
        if ($actual -ne $Sha256.ToLower()) {
            Write-Error "Checksum mismatch. Expected $Sha256, got $actual. The download was not installed."
            exit $EXIT_CHECKSUM
        }

        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        $target = Join-Path $InstallDir 'hitl-daemon.exe'
        # Force replaces an existing daemon; re-running the installer upgrades.
        Move-Item -Path $downloaded -Destination $target -Force

        $version = 'unknown'
        try { $version = (& $target --version 2>$null) } catch { }
        Write-Host "Installed $version to $target"

        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($userPath -notlike "*$InstallDir*") {
            Write-Host ''
            Write-Host "Note: $InstallDir is not on your PATH. Add it, or run the daemon by full path."
        }

        Write-Host ''
        Write-Host 'Run it with:'
        Write-Host "  $target"
    }
    finally {
        if (Test-Path $tmp) { Remove-Item -Path $tmp -Recurse -Force -ErrorAction SilentlyContinue }
    }
}
