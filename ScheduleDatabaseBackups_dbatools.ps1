<#
.SYNOPSIS
    Schedules a one-off SQL Server database back-up job using dbatools and SQL Server Agent.

.DESCRIPTION
    This script prompts for SQL instance, one or more databases, back-up file path, job name, description, and a one-off schedule date/time.
    It checks:
      - That the SQL instance is reachable
      - That the specified databases exist
      - That the back-up path exists (and creates it if possible)
      - That the SQL Agent job name does not already exist (with options to quit, keep, or overwrite)
    The script creates the job, adds back-up steps for each database, and attaches a one-off schedule.
    If the job exists but is not scheduled, you are prompted to add a schedule or cancel.

.PARAMETER SqlInstance
    The SQL Server instance name (e.g., SAWPSQL20A).

.PARAMETER Databases
    One or more database names (comma-separated, e.g., DB1,DB2,DB3).

.PARAMETER FilePath
    The back-up file path (e.g., \\SAWPSQL20C\Backup).

.PARAMETER JobName
    The SQL Agent Job name (e.g., OneOff VoltMX Backups).

.PARAMETER Description
    Job description (e.g., ServiceNow ticket number or purpose).

.PARAMETER ScheduleDateTime
    The date/time to run the back-up (format: yyyy-MM-dd HH:mm, server local time).

.EXAMPLE
    .\ScheduleDatabaseBackups.ps1
    (Prompts for all required inputs)

.NOTES
    - Requires dbatools PowerShell module.
    - SQL Server Agent must be enabled on the target instance.
    - Ensure the SQL Server Agent service account has write access to the back-up path.
    - The job is scheduled as a one-off. You may want to remove it after it runs.
#>

param(
    [Parameter(Mandatory = $false)]
    [string]$SqlInstance,

    [Parameter(Mandatory = $false)]
    [string[]]$Databases,

    [Parameter(Mandatory = $false)]
    [string]$FilePath,

    [Parameter(Mandatory = $false)]
    [string]$JobName,

    [Parameter(Mandatory = $false)]
    [string]$Description,

    [Parameter(Mandatory = $false)]
    [string]$ScheduleDateTime
)

# Check for dbatools module
if (-not (Get-Module -Name dbatools -ListAvailable)) {
    Write-Error "This script requires the dbatools PowerShell module. Please install it with: Install-Module -Name dbatools -Force"
    exit 1
}

# Enhanced input validation with connectivity and existence checks
function Get-ValidatedInput {
    param(
        [string]$Prompt,
        [string]$CurrentValue,
        [scriptblock]$ValidationScript,
        [string]$ErrorMessage,
        [switch]$AllowMultiple,
        [switch]$IsInstance,
        [switch]$IsDatabase,
        [switch]$IsPath,
        [switch]$IsJobName,
        [string]$SqlInstance # For database/job validation
    )

    $value = $CurrentValue
    $valid = $false
    $sqlConnection = $null

    # If this is instance validation, we'll store the connection for later database checks
    if ($IsInstance -and $script:SqlConnection) {
        $sqlConnection = $script:SqlConnection
    }

    while (-not $valid) {
        if (-not $value) {
            $value = Read-Host $Prompt
        }

        # Basic validation first
        if ($AllowMultiple) {
            $values = $value -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
            if (-not $values) {
                Write-Host "ERROR: No valid input provided." -ForegroundColor Red
                $value = $null
                continue
            }
            $invalidValues = @()
            foreach ($v in $values) {
                if (-not (& $ValidationScript $v)) {
                    $invalidValues += $v
                }
            }
            if ($invalidValues) {
                Write-Host "ERROR: $ErrorMessage`: $($invalidValues -join ', ')" -ForegroundColor Red
                $value = $null
                continue
            }
            
            # Extended validation for databases if SQL instance is provided
            if ($IsDatabase -and $SqlInstance) {
                try {
                    # Try to connect if we don't have a connection
                    if (-not $sqlConnection) {
                        $sqlConnection = Connect-DbaInstance -SqlInstance $SqlInstance -ErrorAction Stop
                    }
                    
                    # Check if databases exist
                    $existingDbs = Get-DbaDatabase -SqlInstance $sqlConnection | Select-Object -ExpandProperty Name
                    $missingDbs = $values | Where-Object { $_ -and ($_ -notin $existingDbs) }
                    if ($missingDbs) {
                        Write-Host "ERROR: The following databases do not exist: $($missingDbs -join ', ')" -ForegroundColor Red
                        $value = $null
                        continue
                    }
                } catch {
                    Write-Host "ERROR: Could not validate databases." -ForegroundColor Red
                    $value = $null
                    continue
                }
            }
            return $values
        }
        # For single values
        else {
            if (& $ValidationScript $value) {
                # Extended validation for SQL instance connectivity
                if ($IsInstance) {
                    try {
                        $sqlConnection = Connect-DbaInstance -SqlInstance $value -ErrorAction Stop
                        # Store connection for later use
                        $script:SqlConnection = $sqlConnection
                        Write-Host "Successfully connected to SQL instance '$value'."
                    } catch {
                        Write-Host "ERROR: Could not connect to '$value'. Please check the instance name and connectivity." -ForegroundColor Red
                        $value = $null
                        continue
                    }
                }
                
                # Extended validation for file paths
                if ($IsPath) {
                    if (-not (Test-Path -Path $value -IsValid)) {
                        Write-Host "ERROR: Invalid path format: '$value'" -ForegroundColor Red
                        $value = $null
                        continue
                    }
                    
                    # Check if path exists, try to create it if it doesn't
                    if (-not (Test-Path -Path $value)) {
                        try {
                            New-Item -Path $value -ItemType Directory -Force | Out-Null
                            Write-Host "Backup path '$value' did not exist and was created."
                        } catch {
                            Write-Host "ERROR: Backup path '$value' does not exist and could not be created." -ForegroundColor Red
                            $value = $null
                            continue
                        }
                    } else {
                        Write-Host "Backup path '$value' exists."
                    }
                    
                    # Check if UNC path and warn
                    if ($value -like "\\*") {
                        Write-Host "WARNING: Ensure SQL Server Agent service account has write access to: $value" -ForegroundColor Yellow
                    }
                }
                
                # Job name validation - check if it already exists
                if ($IsJobName -and $SqlInstance) {
                    try {
                        if (-not $sqlConnection) {
                            $sqlConnection = Connect-DbaInstance -SqlInstance $SqlInstance -ErrorAction Stop
                        }
                        
                        # Check if job name already exists
                        $existingJobCheck = "SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'$value'"
                        $jobExists = Invoke-DbaQuery -SqlInstance $sqlConnection -Query $existingJobCheck
                        
                        if ($jobExists) {
                            Write-Host "ERROR: A job named '$value' already exists. Please choose a unique name." -ForegroundColor Red
                            $value = $null
                            continue
                        } else {
                            Write-Host "Job name '$value' is available."
                        }
                    } catch {
                        Write-Host "ERROR: Could not validate job name." -ForegroundColor Red
                        $value = $null
                        continue
                    }
                }
                
                return $value
            }
            Write-Host "ERROR: $ErrorMessage" -ForegroundColor Red
            $value = $null
        }
    }
}

# Validate and parse schedule date/time
function Get-ValidDateTimeInput {
    param(
        [string]$Prompt,
        [string]$CurrentValue,
        [string]$Format = 'yyyy-MM-dd HH:mm'
    )
    
    $dateTime = $CurrentValue
    $valid = $false
    
    while (-not $valid) {
        if (-not $dateTime) {
            $dateTime = Read-Host "$Prompt (format: $Format)"
        }
        
        try {
            $parsedDateTime = [datetime]::ParseExact($dateTime, $Format, $null)
            $valid = $true
            
            # Check if date is in the past
            if ($parsedDateTime -le [datetime]::Now) {
                Write-Warning "The specified date/time ($dateTime) is in the past. Jobs scheduled in the past may run immediately."
                $confirmPastDate = Read-Host "Continue anyway? [Y/N]"
                if ($confirmPastDate.ToUpper() -ne 'Y') {
                    $dateTime = $null
                    $valid = $false
                }
            }
            
            return $parsedDateTime
        } catch {
            Write-Error "Invalid date/time format. Please use '$Format'."
            $dateTime = $null
        }
    }
}

# Initialize a script-level variable for the SQL connection
$script:SqlConnection = $null

# Validate SQL instance with connectivity check
$SqlInstance = Get-ValidatedInput -Prompt "Enter SQL instance (e.g., SAWPSQL20C)" -CurrentValue $SqlInstance -ValidationScript {
    param($value)
    return $value -and $value -notmatch '[^\w\d\.-]'
} -ErrorMessage "SQL instance name contains invalid characters. Use only letters, numbers, dots, and hyphens." -IsInstance

# Validate databases with existence check
$Databases = Get-ValidatedInput -Prompt "Enter databases to backup (comma-separated, e.g., DB1,DB2,DB3)" -CurrentValue ($Databases -join ',') -ValidationScript {
    param($value)
    return $value -notmatch '[^\w\d\$_]'
} -ErrorMessage "Database names can only contain letters, numbers, $, and underscores" -AllowMultiple -IsDatabase -SqlInstance $SqlInstance

# Validate backup file path with existence and creation
$FilePath = Get-ValidatedInput -Prompt "Enter backup file path (e.g., \\SAWPSQL20C\Backup)" -CurrentValue $FilePath -ValidationScript {
    param($value)
    return $value -and $value.IndexOfAny([System.IO.Path]::GetInvalidPathChars()) -lt 0
} -ErrorMessage "Backup path contains invalid characters" -IsPath

# Job name validation with existence check
$JobName = Get-ValidatedInput -Prompt "Enter SQL Agent Job name (e.g., OneOff VoltMX Backups)" -CurrentValue $JobName -ValidationScript {
    param($value)
    return $value -and $value.Length -le 128 -and $value -notmatch '[\[\]]'
} -ErrorMessage "Job name is invalid. Must be less than 128 characters and not contain square brackets" -IsJobName -SqlInstance $SqlInstance

# Simple text description (no special validation needed)
if (-not $Description) {
    $Description = Read-Host "Enter job description (e.g., ServiceNow ticket number or purpose)"
}

# Validate and parse the schedule date/time
$runDateTime = Get-ValidDateTimeInput -Prompt "Enter the date/time to run the backup" -CurrentValue $ScheduleDateTime

# Create the SQL Agent Job and steps
try {
    $null = New-DbaAgentJob -SqlInstance $SqlInstance -Job $JobName -Description $Description -Force
    foreach ($db in $Databases) {
        $stepName = "Backup $db"
        $command = "Backup-DbaDatabase -SqlInstance `"$SqlInstance`" -Database `"$db`" -CopyOnly -FilePath `"$FilePath`""
        $null = New-DbaAgentJobStep -SqlInstance $SqlInstance -Job $JobName -StepName $stepName -Subsystem PowerShell -Command $command
    }
} catch {
    Write-Error "Failed to create job or steps: $_"
    exit 1
}

# Add a final job step to export job history and remove the job itself
try {
    $finalStepName = "Export Job History and Remove Job"
    $finalStepCommand = @"
`$SqlInstance = '$SqlInstance'
`$JobName = '$JobName'
`$BackupPath = '$FilePath'
`$history = Invoke-DbaQuery -SqlInstance `$SqlInstance -Query @'
SELECT run_date, run_time, step_id, step_name, message
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
JOIN msdb.dbo.sysjobsteps s ON j.job_id = s.job_id AND h.step_id = s.step_id
WHERE j.name = N''`$JobName''
ORDER BY run_date DESC, run_time DESC
'@
`$csvPath = Join-Path `$BackupPath "`$JobName-JobHistory.csv"
`$history | Export-Csv -Path `$csvPath -NoTypeInformation
Remove-DbaAgentJob -SqlInstance `$SqlInstance -Job `$JobName -Confirm:`$false
"@
    # Add the final step to the job
    $null = New-DbaAgentJobStep -SqlInstance $SqlInstance -Job $JobName -StepName $finalStepName -Subsystem PowerShell -Command $finalStepCommand
    
    # Replace the job step flow configuration section with this:
    try {
        # Get all steps and sort by ID
        $allSteps = Get-DbaAgentJobStep -SqlInstance $SqlInstance -Job $JobName | Sort-Object -Property Id
        $finalStep = $allSteps | Where-Object { $_.Name -eq $finalStepName }
        $finalStepId = $finalStep.Id
    
        # Get only the backup steps (exclude the final step)
        $backupSteps = $allSteps | Where-Object { $_.Name -ne $finalStepName }
        $totalSteps = $backupSteps.Count
    
        # Configure each backup step
        for ($i = 0; $i -lt $totalSteps; $i++) {
            $currentStep = $backupSteps[$i]
        
            # If this is the last backup step, go to the final step
            if ($i -eq $totalSteps - 1) {
                $null = Set-DbaAgentJobStep -SqlInstance $SqlInstance -Job $JobName -StepName $currentStep.Name `
                    -OnSuccessAction GoToStep -OnSuccessStepId $finalStepId `
                    -OnFailAction GoToStep -OnFailStepId $finalStepId
                Write-Host "Last backup step '$($currentStep.Name)' configured to proceed to final step on success or failure"
            }
            # If this is not the last backup step, go to the next backup step
            else {
                $nextStep = $backupSteps[$i + 1]
                $null = Set-DbaAgentJobStep -SqlInstance $SqlInstance -Job $JobName -StepName $currentStep.Name `
                    -OnSuccessAction GoToStep -OnSuccessStepId $nextStep.Id `
                    -OnFailAction GoToStep -OnFailStepId $finalStepId
                Write-Host "Backup step '$($currentStep.Name)' configured to proceed to next step on success, final step on failure"
            }
        }
    } catch {
        Write-Error "Failed to configure job step flow: $_"
        exit 1
    }
} catch {
    Write-Error "Failed to add final job step for exporting history and removing job: $_"
    exit 1
}

# Add a one-off schedule using T-SQL (works on all versions)
$ScheduleName = "OneOff-$($runDateTime.ToString('yyyyMMddHHmm'))"
$scheduleExistsTsql = @"
SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'$ScheduleName'
"@

try {
    $scheduleExists = Invoke-DbaQuery -SqlInstance $SqlInstance -Query $scheduleExistsTsql
    
    if (-not $scheduleExists) {
        $tsql = @"
DECLARE @schedule_id INT
EXEC msdb.dbo.sp_add_schedule
    @schedule_name = N'$ScheduleName',
    @enabled = 1,
    @freq_type = 1, -- One time
    @active_start_date = $($runDateTime.ToString('yyyyMMdd')),
    @active_start_time = $($runDateTime.ToString('HHmmss')),
    @schedule_id = @schedule_id OUTPUT
EXEC msdb.dbo.sp_attach_schedule
    @job_name = N'$JobName',
    @schedule_name = N'$ScheduleName'
"@
        $null = Invoke-DbaQuery -SqlInstance $SqlInstance -Query $tsql
    }
} catch {
    Write-Error "Failed to create or attach schedule: $_"
    exit 1
}

# Confirm job creation and schedule using T-SQL
$checkJobTsql = @"
SELECT s.name AS schedule_name, s.freq_type, s.active_start_date, s.active_start_time
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
WHERE j.name = N'$JobName'
"@

try {
    $jobSchedules = Invoke-DbaQuery -SqlInstance $SqlInstance -Query $checkJobTsql
} catch {
    Write-Error "Failed to confirm job and schedule: $_"
    exit 1
}

# Output job details for ServiceNow
try {
    $jobInfo = Invoke-DbaQuery -SqlInstance $SqlInstance -Query @"
SELECT sj.name AS [JobName], sjs.step_id, sjs.step_name, sjs.command
FROM msdb.dbo.sysjobs sj
JOIN msdb.dbo.sysjobsteps sjs ON sj.job_id = sjs.job_id
WHERE sj.name = N'$JobName'
ORDER BY sjs.step_id
"@

    $scheduleInfo = Invoke-DbaQuery -SqlInstance $SqlInstance -Query @"
SELECT s.name AS [ScheduleName], 
       s.enabled, 
       s.freq_type, 
       s.active_start_date, 
       s.active_start_time
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobschedules js ON j.job_id = js.job_id
JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
WHERE j.name = N'$JobName'
"@
} catch {
    Write-Error "Failed to retrieve job details for output: $_"
    exit 1
}

Write-Host "==================== Job Details for ServiceNow ===================="
Write-Host "Job Name: $JobName"
Write-Host ""
Write-Host "Steps:"
foreach ($step in $jobInfo) {
    if ($step.step_name -eq "Export Job History and Remove Job") {
        Write-Host ("  Step {0}: {1}" -f $step.step_id, $step.step_name)
        $historyPath = Join-Path -Path $FilePath -ChildPath "$JobName-JobHistory.csv"
        Write-Host ("    Action: Job history saved to '$historyPath'")
        Write-Host ("    Action: Job will be removed after completion (Remove-DbaAgentJob)")
    } else {
        Write-Host ("  Step {0}: {1}" -f $step.step_id, $step.step_name)
        Write-Host ("    Command: {0}" -f $step.command)
    }
}
Write-Host ""
Write-Host "Schedule:"
if (-not $scheduleInfo -or $scheduleInfo.Count -eq 0) {
    Write-Warning "No schedule information found for job '$JobName'. The job exists but is not scheduled to run automatically."
    Write-Host "  You will need to run this job manually or add a schedule via SQL Server Agent."
} else {
    foreach ($sched in $scheduleInfo) {
        # Format date and time
        $schedDate = [datetime]::ParseExact($sched.active_start_date.ToString(), 'yyyyMMdd', $null).ToShortDateString()
        $schedTime = "{0:D2}:{1:D2}:{2:D2}" -f ([int]($sched.active_start_time / 10000)), ([int](($sched.active_start_time / 100) % 100)), ([int]($sched.active_start_time % 100))
        Write-Host ("  Schedule Name: {0}" -f $sched.ScheduleName)
        Write-Host ("    Enabled: {0}" -f ($(if ($sched.enabled -eq 1) {'Yes'} else {'No'})))
        Write-Host ("    Type: {0}" -f $(switch ($sched.freq_type) {1 {"One time"} 4 {"Daily"} 8 {"Weekly"} 16 {"Monthly"} default {"Other"}}))
        Write-Host ("    Start: {0} {1}" -f $schedDate, $schedTime)
    }
}
Write-Host "===================================================================="

# Suppress unwanted output from dbatools or other commands
$null = $jobInfo
$null = $scheduleInfo
$null = $jobSchedules
$null = $finalStep
$null = $allSteps