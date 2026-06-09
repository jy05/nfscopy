<#
sync-nfs.ps1 - verified, parallel move of files between two NFS mounts.

Pipeline per run:
  1. Acquire exclusive lock on flock.lock; exit if another run holds it.
  2. Manifest: source files untouched for MinAgeMinutes AND size/mtime stable
     across a ProbeSeconds window. In-flight files wait for the next run.
  3. Workers (parallel, per file): copy to Processing, re-apply source
     timestamps/attributes, MD5-verify source vs copy, rename Processing ->
     Final (same mount), then delete that exact source file.
  4. Remove empty directories, deepest first, non-recursively - a directory
     still holding an in-flight file fails the delete and survives.

Exit code 0 = clean run (or skipped due to lock), 1 = one or more files failed.
#>

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingBrokenHashAlgorithms', '',
    Justification = 'MD5 is the verification algorithm required by the task spec. It guards transfer integrity, not a security boundary.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '',
    Justification = 'Console echo for interactive runs only; the log file is the record and Write-Host keeps the output stream clean for worker results.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingEmptyCatchBlock', '',
    Justification = 'Intentional in directory cleanup: DirectoryInfo.Delete() throwing on a non-empty dir is the safety mechanism that protects folders holding in-flight files.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'LogFile',
    Justification = 'False positive: LogFile is used by the Write-Log function nested inside the worker scriptblock; the rule does not trace nested function scopes.')]
[CmdletBinding()]
param(
    [string]$SourceRoot     = 'S:\incoming',
    [string]$ProcessingRoot = 'T:\processing',
    [string]$FinalRoot      = 'T:\main',
    [string]$LockFile       = 'C:\sync-nfs\flock.lock',
    [string]$LogDir         = 'C:\sync-nfs\logs',
    [int]$MinAgeMinutes     = 5,    # file must be untouched at least this long
    [int]$ProbeSeconds      = 30,   # size/mtime must not change across this window
    [int]$Workers           = 8     # parallel per-file pipelines
)

$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path $LogDir, (Split-Path $LockFile) | Out-Null
$logFile = Join-Path $LogDir ("sync-{0:yyyyMMdd}.log" -f (Get-Date))

# Mutex serializes log writes across parallel workers.
function Write-Log {
    param([string]$Message)
    $line  = "{0:yyyy-MM-dd HH:mm:ss}  {1}" -f (Get-Date), $Message
    $mutex = New-Object System.Threading.Mutex($false, 'Local\sync-nfs-log')
    [void]$mutex.WaitOne()
    try     { Add-Content -LiteralPath $logFile -Value $line; Write-Host $line }
    finally { $mutex.ReleaseMutex(); $mutex.Dispose() }
}

# Per-file pipeline executed by each runspace worker. Returns 'OK' or 'FAIL'.
# $Src is the FileInfo captured at manifest time, so the timestamps re-applied
# below are the pre-copy originals.
$workerScript = {
    param($Src, $SrcPrefix, $ProcessingRoot, $FinalRoot, $LogFile)

    function Write-Log {
        param([string]$Message)
        $line  = "{0:yyyy-MM-dd HH:mm:ss}  {1}" -f (Get-Date), $Message
        $mutex = New-Object System.Threading.Mutex($false, 'Local\sync-nfs-log')
        [void]$mutex.WaitOne()
        try     { Add-Content -LiteralPath $LogFile -Value $line }
        finally { $mutex.ReleaseMutex(); $mutex.Dispose() }
    }

    $rel  = $Src.FullName.Substring($SrcPrefix.Length).TrimStart('\')
    $proc = Join-Path $ProcessingRoot $rel
    $dest = Join-Path $FinalRoot $rel
    try {
        if (Test-Path -LiteralPath $dest) {
            # A prior run may have died after renaming into Final but before
            # deleting the source. If contents match, finish that job so the
            # source file cannot be stranded on the space-limited src mount.
            $destHash = (Get-FileHash -LiteralPath $dest         -Algorithm MD5).Hash
            $srcHash  = (Get-FileHash -LiteralPath $Src.FullName -Algorithm MD5).Hash
            if ($destHash -ne $srcHash) {
                throw "destination exists with DIFFERENT content - not overwriting"
            }
            Remove-Item -LiteralPath $Src.FullName -Force
            Write-Log -Message "OK    $rel - already in Final (hash match), source removed"
            return 'OK'
        }

        New-Item -ItemType Directory -Force -Path (Split-Path $proc) | Out-Null
        Copy-Item -LiteralPath $Src.FullName -Destination $proc -Force

        # Copy-Item preserves content + LastWriteTime only; carry the rest.
        $p = Get-Item -LiteralPath $proc
        $p.CreationTimeUtc   = $Src.CreationTimeUtc
        $p.LastWriteTimeUtc  = $Src.LastWriteTimeUtc
        $p.LastAccessTimeUtc = $Src.LastAccessTimeUtc
        $p.Attributes        = $Src.Attributes

        # Hash source AFTER copying: if the source changed mid-copy despite
        # the stability check, the hashes diverge and the file fails safe.
        $srcHash  = (Get-FileHash -LiteralPath $Src.FullName -Algorithm MD5).Hash
        $procHash = (Get-FileHash -LiteralPath $proc         -Algorithm MD5).Hash
        if ($srcHash -ne $procHash) {
            throw "MD5 mismatch src=$srcHash processing=$procHash"
        }

        New-Item -ItemType Directory -Force -Path (Split-Path $dest) | Out-Null
        Move-Item -LiteralPath $proc -Destination $dest   # same mount = rename

        Remove-Item -LiteralPath $Src.FullName -Force      # this file only
        Write-Log -Message "OK    $rel  MD5=$srcHash"
        'OK'
    } catch {
        Write-Log -Message "FAIL  $rel - $_"
        'FAIL'
        # Source left untouched; stale processing copy is overwritten
        # (-Force) on the next attempt.
    }
}

# --- 1. Lock: the held exclusive handle IS the lock. The file itself stays. ---
try {
    $lock = [System.IO.File]::Open($LockFile, 'OpenOrCreate', 'ReadWrite', 'None')
} catch {
    Write-Log -Message "Another instance holds $LockFile - exiting."
    exit 0
}

$failCount = 0
try {
    Write-Log -Message "Run started. Source=$SourceRoot Processing=$ProcessingRoot Final=$FinalRoot Workers=$Workers"

    # --- 2. Manifest of stable files ---
    $cutoff   = (Get-Date).AddMinutes(-$MinAgeMinutes)
    $snapshot = Get-ChildItem -LiteralPath $SourceRoot -Recurse -File |
                Where-Object { $_.LastWriteTime -lt $cutoff }

    Start-Sleep -Seconds $ProbeSeconds

    $manifest = @(foreach ($f in $snapshot) {
        $now = Get-Item -LiteralPath $f.FullName -ErrorAction SilentlyContinue
        if ($now -and
            $now.Length -eq $f.Length -and
            $now.LastWriteTimeUtc -eq $f.LastWriteTimeUtc) {
            $now
        } else {
            Write-Log -Message "SKIP  $($f.FullName) - changed during probe window (in-flight)"
        }
    })
    Write-Log -Message "Manifest: $($manifest.Count) stable file(s)."

    # --- 3. Dispatch manifest to the runspace pool ---
    $srcPrefix = $SourceRoot.TrimEnd('\')
    $pool      = [runspacefactory]::CreateRunspacePool(1, $Workers)
    $pool.Open()
    try {
        $jobs = foreach ($src in $manifest) {
            $ps = [powershell]::Create()
            $ps.RunspacePool = $pool
            [void]$ps.AddScript($workerScript.ToString()).
                AddArgument($src).AddArgument($srcPrefix).
                AddArgument($ProcessingRoot).AddArgument($FinalRoot).
                AddArgument($logFile)
            New-Object psobject -Property @{ PS = $ps; Handle = $ps.BeginInvoke() }
        }
        foreach ($job in $jobs) {
            $result = $job.PS.EndInvoke($job.Handle)   # blocks until that worker is done
            if ($result -contains 'FAIL') { $failCount++ }
            $job.PS.Dispose()
        }
    } finally {
        $pool.Close()
        $pool.Dispose()
    }

    # --- 4. Empty-directory cleanup: non-recursive delete, deepest first. ---
    # DirectoryInfo.Delete() throws on non-empty dirs; that failure is the
    # safety mechanism protecting dirs that still hold in-flight files.
    foreach ($root in @($SourceRoot, $ProcessingRoot)) {
        Get-ChildItem -LiteralPath $root -Recurse -Directory |
            Sort-Object { $_.FullName.Length } -Descending |
            ForEach-Object {
                try { $_.Delete(); Write-Log -Message "RMDIR $($_.FullName)" } catch { }
            }
    }

    Write-Log -Message "Run finished. Failures: $failCount"
} finally {
    $lock.Dispose()
}

exit ([int]($failCount -gt 0))
