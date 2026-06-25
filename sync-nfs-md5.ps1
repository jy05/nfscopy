<#
sync-nfs.ps1 - verified, parallel move of files between two NFS mounts.

Pipeline per run:
  1. Acquire exclusive lock on flock.lock; exit if another run holds it.
  2. Manifest: source files untouched for MinAgeMinutes AND size/mtime stable
     across a ProbeSeconds window. In-flight files wait for the next run.
  3. Workers (parallel, per file): hash source [INCOMING], copy to Processing,
     re-apply source timestamps/attributes, hash copy [PROCESSING], compare,
     rename Processing -> Final (same mount), hash final [MAIN], delete source.
     Each file's hash lines are written to the log as one grouped block.
  4. Failure handling (collector, single-threaded): a file that fails keeps its
     source in incoming and is retried next run. A persistent attempt counter
     in StateFile survives between runs; on the MaxAttempts-th failure the source
     is moved to FailedRoot, its leftover Processing copy is deleted, and the
     counter entry is cleared so a future same-name file starts fresh. A file is
     keyed by path+size+mtime, so a new file reusing a name gets a fresh count.
  5. Empty-directory cleanup, deepest first, non-recursively - a directory still
     holding an in-flight file fails the delete and survives.

Folder cleanup policy: incoming and processing are kept clear (verified files
leave incoming; processing copies are overwritten on retry, deleted on give-up,
and empty dirs are swept). MAIN is never deleted from - a bad file there means
at-rest corruption and is the only surviving copy. FailedRoot is left for
human inspection and is not auto-cleaned.

Logging is silent unless a file moves, fails, or is given up: empty runs and
in-flight-only runs write nothing.

Exit code 0 = clean run (or skipped due to lock), 1 = one or more files failed.
#>

[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingBrokenHashAlgorithms', '',
    Justification = 'MD5 is the verification algorithm required by the task spec. It guards transfer integrity, not a security boundary.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingWriteHost', '',
    Justification = 'Console echo for interactive runs only; the log file is the record and Write-Host keeps the output stream clean for worker results.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSAvoidUsingEmptyCatchBlock', '',
    Justification = 'Intentional in directory cleanup: DirectoryInfo.Delete() throwing on a non-empty dir is the safety mechanism that protects folders holding in-flight files.')]
[Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'LogFile',
    Justification = 'False positive: LogFile is used by the Write-Block function nested inside the worker scriptblock; the rule does not trace nested function scopes.')]
[CmdletBinding()]
param(
    [string]$SourceRoot     = 'S:\incoming',
    [string]$ProcessingRoot = 'T:\processing',
    [string]$FinalRoot      = 'T:\main',
    [string]$FailedRoot     = 'T:\failed',                      # give-up files (on roomy dest mount)
    [string]$LockFile       = 'C:\sync-nfs\flock.lock',
    [string]$LogDir         = 'C:\sync-nfs\logs',
    [string]$StateFile      = 'C:\sync-nfs\state\attempts.json', # persistent attempt counts
    [int]$MinAgeMinutes     = 5,    # file must be untouched at least this long
    [int]$ProbeSeconds      = 30,   # size/mtime must not change across this window
    [int]$Workers           = 8,    # parallel per-file pipelines
    [int]$MaxAttempts       = 3     # failures before a file is moved to FailedRoot
)

$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path `
    $LogDir, $FailedRoot, (Split-Path $LockFile), (Split-Path $StateFile) | Out-Null
$logFile = Join-Path $LogDir ("sync-{0:yyyyMMdd}.log" -f (Get-Date))

# Write pre-formatted lines as one grouped block (trailing blank line separates
# files). The mutex keeps each block contiguous across the parallel workers and
# the collector. Nothing is written unless there are lines to report.
function Write-Block {
    param([string[]]$Lines)
    $mutex = New-Object System.Threading.Mutex($false, 'Local\sync-nfs-log')
    [void]$mutex.WaitOne()
    try     { Add-Content -LiteralPath $logFile -Value ($Lines + ''); $Lines | ForEach-Object { Write-Host $_ } }
    finally { $mutex.ReleaseMutex(); $mutex.Dispose() }
}

# Move a given-up source file to FailedRoot, mirroring its relative path. Never
# overwrites a prior failed copy of the same name (appends a timestamp instead).
# Cross-mount move is a copy+delete, which is fine for this rare event.
function Move-ToFailed {
    param([string]$SourcePath, [string]$Rel, [string]$FailedRoot)
    $dest = Join-Path $FailedRoot $Rel
    $dir  = Split-Path $dest
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    if (Test-Path -LiteralPath $dest) {
        $stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
        $dest  = Join-Path $dir ("{0}_{1}{2}" -f `
            [IO.Path]::GetFileNameWithoutExtension($dest), $stamp, [IO.Path]::GetExtension($dest))
    }
    Move-Item -LiteralPath $SourcePath -Destination $dest
    $dest
}

# Per-file pipeline executed by each runspace worker. Returns 'OK' or 'FAIL'.
# $Src is the FileInfo captured at manifest time, so the timestamps re-applied
# below are the pre-copy originals. The worker buffers its log lines and writes
# them once, grouped, only when there is a result to report. Failure bookkeeping
# (attempt counts, give-up) is handled by the single-threaded collector, not here.
$workerScript = {
    param($Src, $SrcPrefix, $ProcessingRoot, $FinalRoot, $LogFile)

    function New-Line {  # "<timestamp>  [STAGE]  <relpath>  MD5=<hash>"
        param([string]$Stage, [string]$Rel, [string]$Hash)
        "{0:yyyy-MM-dd HH:mm:ss}  [{1,-10}] {2}  MD5={3}" -f (Get-Date), $Stage, $Rel, $Hash
    }
    function New-Note {  # free-text line: "<timestamp>  [STAGE]  <text>"
        param([string]$Stage, [string]$Text)
        "{0:yyyy-MM-dd HH:mm:ss}  [{1,-10}] {2}" -f (Get-Date), $Stage, $Text
    }
    function Write-Block {
        param([string[]]$Lines)
        $mutex = New-Object System.Threading.Mutex($false, 'Local\sync-nfs-log')
        [void]$mutex.WaitOne()
        try     { Add-Content -LiteralPath $LogFile -Value ($Lines + '') }
        finally { $mutex.ReleaseMutex(); $mutex.Dispose() }
    }

    $rel   = $Src.FullName.Substring($SrcPrefix.Length).TrimStart('\')
    $proc  = Join-Path $ProcessingRoot $rel
    $dest  = Join-Path $FinalRoot $rel
    $block = @()
    try {
        if (Test-Path -LiteralPath $dest) {
            # A file with this name already sits in main. Hash both to tell apart
            # two situations that share a name but mean different things.
            $destHash = (Get-FileHash -LiteralPath $dest         -Algorithm MD5).Hash
            $srcHash  = (Get-FileHash -LiteralPath $Src.FullName -Algorithm MD5).Hash
            if ($destHash -eq $srcHash) {
                # Same name AND same contents: this file is already delivered
                # (e.g. a prior run copied it, then died before clearing the
                # source). Finish that job by clearing the source; copy nothing.
                Remove-Item -LiteralPath $Src.FullName -Force
                Write-Block -Lines @(New-Note -Stage 'RECOVER' -Text "$rel  already in Final (hash match), source removed")
                return 'OK'
            }
            # Same name but DIFFERENT contents: a genuinely different file that
            # happens to share a name (e.g. two sources both named Text1.txt).
            # Keep both by giving the incoming one a date-time stamp in its name,
            # landing it right where it would have gone. The original is untouched.
            $stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
            $rel   = Join-Path (Split-Path $rel) `
                     ("{0}_{1}{2}" -f [IO.Path]::GetFileNameWithoutExtension($rel), $stamp, [IO.Path]::GetExtension($rel))
            $proc  = Join-Path $ProcessingRoot $rel
            $dest  = Join-Path $FinalRoot $rel
            $block += New-Note -Stage 'COLLISION' -Text "differs from existing main copy; saving incoming as $rel"
        }

        # [INCOMING] hash of the source before copying.
        $incomingHash = (Get-FileHash -LiteralPath $Src.FullName -Algorithm MD5).Hash
        $block += New-Line -Stage 'INCOMING' -Rel $rel -Hash $incomingHash

        New-Item -ItemType Directory -Force -Path (Split-Path $proc) | Out-Null
        Copy-Item -LiteralPath $Src.FullName -Destination $proc -Force

        # Copy-Item preserves content + LastWriteTime only; carry the rest.
        $p = Get-Item -LiteralPath $proc
        $p.CreationTimeUtc   = $Src.CreationTimeUtc
        $p.LastWriteTimeUtc  = $Src.LastWriteTimeUtc
        $p.LastAccessTimeUtc = $Src.LastAccessTimeUtc
        $p.Attributes        = $Src.Attributes

        # [PROCESSING] hash of the copy. If the source changed mid-copy despite
        # the stability check, this differs from INCOMING and the file fails safe
        # BEFORE the move - source untouched, nothing written to main.
        $procHash = (Get-FileHash -LiteralPath $proc -Algorithm MD5).Hash
        $block += New-Line -Stage 'PROCESSING' -Rel $rel -Hash $procHash
        if ($procHash -ne $incomingHash) {
            throw "MD5 mismatch incoming=$incomingHash processing=$procHash"
        }

        New-Item -ItemType Directory -Force -Path (Split-Path $dest) | Out-Null
        Move-Item -LiteralPath $proc -Destination $dest   # same mount = rename

        # [MAIN] hash read from the file in its final location, after the move.
        # The file was touched (renamed), so log its hash as it now sits in main.
        # A same-mount rename preserves content, so this equals PROCESSING under
        # healthy storage; the logged hashes let a human spot at-rest corruption.
        # We do NOT throw on a difference: the verified copy is already in main
        # and is the only surviving copy once the source is released below.
        $mainHash = (Get-FileHash -LiteralPath $dest -Algorithm MD5).Hash
        $block += New-Line -Stage 'MAIN' -Rel $rel -Hash $mainHash

        Remove-Item -LiteralPath $Src.FullName -Force      # this file only
        Write-Block -Lines $block
        'OK'
    } catch {
        # Emit whatever stages completed, plus the failure reason. Source is left
        # untouched; the stale processing copy is overwritten (-Force) on retry.
        $block += New-Note -Stage 'FAIL' -Text "$rel  $_"
        Write-Block -Lines $block
        'FAIL'
    }
}

# --- 1. Lock: the held exclusive handle IS the lock. The file itself stays. ---
    # Plain terms: make sure only one copy of this script runs at a time. We
    # grab an exclusive hold on a small file; if another run already holds it,
    # we quietly stop so two runs can never fight over the same files.
try {
    $lock = [System.IO.File]::Open($LockFile, 'OpenOrCreate', 'ReadWrite', 'None')
} catch {
    exit 0   # another instance is running; nothing to log
}

$failCount = 0
try {
    # --- 2. Manifest of stable files ---
    # Plain terms: build the list of files that are safe to move this run. A file
    # qualifies only if it has sat still for a while AND its size and timestamp
    # stay identical across a short wait. A file still being written keeps
    # changing, so it is left out and picked up on a later run.
    $cutoff   = (Get-Date).AddMinutes(-$MinAgeMinutes)
    $snapshot = Get-ChildItem -LiteralPath $SourceRoot -Recurse -File |
                Where-Object { $_.LastWriteTime -lt $cutoff }

    Start-Sleep -Seconds $ProbeSeconds

    # In-flight files (changed during the probe window) are silently excluded.
    $manifest = @(foreach ($f in $snapshot) {
        $now = Get-Item -LiteralPath $f.FullName -ErrorAction SilentlyContinue
        if ($now -and
            $now.Length -eq $f.Length -and
            $now.LastWriteTimeUtc -eq $f.LastWriteTimeUtc) {
            $now
        }
    })

    # Nothing stable to move: exit silently, writing no log.
    if ($manifest.Count -gt 0) {
        $srcPrefix = $SourceRoot.TrimEnd('\')

        # Load persistent attempt counts once (normalize to hashtable entries).
        $attempts   = @{}
        $stateDirty = $false
        if (Test-Path -LiteralPath $StateFile) {
            $raw = Get-Content -LiteralPath $StateFile -Raw
            if ($raw.Trim()) {
                (ConvertFrom-Json $raw).PSObject.Properties | ForEach-Object {
                    $attempts[$_.Name] = @{ count = [int]$_.Value.count; sig = [string]$_.Value.sig }
                }
            }
        }

        # --- 3. Dispatch manifest to the runspace pool ---
        # Plain terms: hand each file to a pool of background workers so several
        # files copy and verify at the same time instead of one after another.
        $pool = [runspacefactory]::CreateRunspacePool(1, $Workers)
        $pool.Open()
        try {
            $jobs = foreach ($src in $manifest) {
                $ps = [powershell]::Create()
                $ps.RunspacePool = $pool
                [void]$ps.AddScript($workerScript.ToString()).
                    AddArgument($src).AddArgument($srcPrefix).
                    AddArgument($ProcessingRoot).AddArgument($FinalRoot).
                    AddArgument($logFile)
                [pscustomobject]@{ PS = $ps; Handle = $ps.BeginInvoke(); Src = $src }
            }

            # --- 4. Collect results (single-threaded) and handle failures ---
            # Plain terms: gather each worker's result one at a time. On success we
            # clear that file's failure record. On failure we count the attempt, and
            # once a file has failed enough times we move it aside to the failed
            # folder, remove its leftover copy from processing, and stop retrying it.
            foreach ($job in $jobs) {
                $result = $job.PS.EndInvoke($job.Handle)   # blocks until that worker is done
                $job.PS.Dispose()
                $rel = $job.Src.FullName.Substring($srcPrefix.Length).TrimStart('\')

                if ($result -notcontains 'FAIL') {
                    # Success: clear any prior failure record for this path.
                    if ($attempts.ContainsKey($rel)) { $attempts.Remove($rel); $stateDirty = $true }
                    continue
                }

                $failCount++
                $src  = $job.Src
                $sig  = "$($src.Length):$($src.LastWriteTimeUtc.Ticks)"   # file identity
                $prev = $attempts[$rel]
                # A different size/mtime means a new file reusing the name: fresh count.
                if ($prev -and $prev.sig -eq $sig) { $count = [int]$prev.count + 1 } else { $count = 1 }

                if ($count -ge $MaxAttempts) {
                    # Give up: move source out of incoming, delete the processing
                    # leftover (so it cannot orphan now that retries have stopped),
                    # and clear the counter so a future same-name file restarts.
                    $attempts.Remove($rel); $stateDirty = $true
                    try {
                        $movedTo  = Move-ToFailed -SourcePath $src.FullName -Rel $rel -FailedRoot $FailedRoot
                        $leftover = Join-Path $ProcessingRoot $rel
                        if (Test-Path -LiteralPath $leftover) { Remove-Item -LiteralPath $leftover -Force }
                        Write-Block -Lines @(("{0:yyyy-MM-dd HH:mm:ss}  [{1,-10}] {2}" -f `
                            (Get-Date), 'GIVEUP', "$rel  failed ${MaxAttempts}x; source -> $movedTo; processing copy removed"))
                    } catch {
                        Write-Block -Lines @(("{0:yyyy-MM-dd HH:mm:ss}  [{1,-10}] {2}" -f `
                            (Get-Date), 'GIVEUP', "$rel  reached ${MaxAttempts}x but move to failed FAILED: $_"))
                    }
                } else {
                    $attempts[$rel] = @{ count = $count; sig = $sig }; $stateDirty = $true
                }
            }
        } finally {
            $pool.Close()
            $pool.Dispose()
        }

        # Persist attempt counts only if they changed.
        if ($stateDirty) {
            if ($attempts.Count) { ($attempts | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $StateFile -Encoding UTF8 }
            else                 { '{}'                                  | Set-Content -LiteralPath $StateFile -Encoding UTF8 }
        }

        # --- 5. Empty-directory cleanup: non-recursive delete, deepest first. ---
        # Plain terms: tidy up by deleting folders that are now empty. The delete is
        # deliberately the kind that refuses to remove a folder that still holds a
        # file, which protects any folder whose large file is still arriving.
        # DirectoryInfo.Delete() throws on non-empty dirs; that failure is the
        # safety mechanism protecting dirs that still hold in-flight files. MAIN
        # is intentionally not swept for content - only incoming and processing.
        foreach ($root in @($SourceRoot, $ProcessingRoot)) {
            Get-ChildItem -LiteralPath $root -Recurse -Directory |
                Sort-Object { $_.FullName.Length } -Descending |
                ForEach-Object {
                    try { $_.Delete() } catch { }
                }
        }
    }
} finally {
    $lock.Dispose()
}

exit ([int]($failCount -gt 0))
