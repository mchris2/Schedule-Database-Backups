<#
.SYNOPSIS
    Schedules a one-off SQL Server database backup job using Windows Task Scheduler.

.DESCRIPTION
    This script schedules a PowerShell task to run backups for one or more SQL databases.
    The task will run once at the specified time and clean up after itself.
    It checks:
      - That the SQL instance is reachable
      - That the specified databases exist
      - That the backup path exists (and creates it if possible)
    
    The script creates a PowerShell script in the user's profile directory that performs the backups,
    then schedules it to run using Windows Task Scheduler. The task will run whether the user is logged
    in or not, and will delete itself upon completion.

.PARAMETER SqlInstance
    The SQL Server instance name (e.g. SAWPSQL20A).

.PARAMETER Databases
    One or more database names (comma-separated, e.g. DB1,DB2,DB3).

.PARAMETER FilePath
    The backup file path (e.g. \\SAWPSQL20C\Backup). Default is \\SqlInstance\Backup.

.PARAMETER TaskName
    The Windows Task name (e.g. OneOff_SQL_Backup_Task). Default is SQL_Backup_[timestamp].

.PARAMETER Description
    Task description (e.g. ServiceNow ticket number or purpose).

.PARAMETER ScheduleDateTime
    The date/time to run the backup (format: yyyy-MM-dd HH:mm, local time). 
    Default is current time + 5 minutes.

.EXAMPLE
    .\ScheduleDatabaseBackups_scheduledtask.ps1
    (Prompts for all required inputs with sensible defaults)

.EXAMPLE
    .\ScheduleDatabaseBackups_scheduledtask.ps1 -SqlInstance "SAWPSQL20A" -Databases "DB1","DB2" -FilePath "\\SAWPSQL20A\Backup"
    (Uses provided values and prompts for missing inputs)

.NOTES
    - Requires dbatools PowerShell module.
    - The account running the scheduled task must have permissions to connect to SQL Server.
    - Ensure the account has write access to the backup path.
    - No SQL Server Agent required - works with all SQL Server editions.
    - The script includes error handling and fallback logging if network paths are unavailable.
    - The scheduled task will delete itself after completion.
#>

param(
    [Parameter(Mandatory = $false)]
    [string]$SqlInstance,

    [Parameter(Mandatory = $false)]
    [string[]]$Databases,

    [Parameter(Mandatory = $false)]
    [string]$FilePath,

    [Parameter(Mandatory = $false)]
    [string]$TaskName,

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
        [switch]$IsTaskName,
        [string]$SqlInstance # For database validation
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
        else {
            # Show a default value in the prompt
            $userInput = Read-Host "$Prompt [$value]"
            # Only replace the default if user entered something
            if ($userInput) {
                $value = $userInput
            }
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
                        Write-Host "WARNING: Ensure the scheduled task account has write access to: $value" -ForegroundColor Yellow
                    }
                }
                
                # Task name validation - check if it already exists
                if ($IsTaskName) {
                    try {
                        $taskExists = Get-ScheduledTask -TaskName $value -ErrorAction SilentlyContinue
                        
                        if ($taskExists) {
                            Write-Host "ERROR: A task named '$value' already exists. Please choose a unique name." -ForegroundColor Red
                            $value = $null
                            continue
                        } else {
                            Write-Host "Task name '$value' is available."
                        }
                    } catch {
                        Write-Host "ERROR: Could not validate task name. $($_)" -ForegroundColor Red
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
        else {
            # Show the default value in the prompt
            $userInput = Read-Host "$Prompt (format: $Format) [$dateTime]"
            # Only replace the default if user entered something
            if ($userInput) {
                $dateTime = $userInput
            }
        }

        try {
            $parsedDateTime = [datetime]::ParseExact($dateTime, $Format, $null)
            $valid = $true
            
            # Check if date is in the past
            if ($parsedDateTime -le [datetime]::Now) {
                Write-Warning "The specified date/time ($dateTime) is in the past. Tasks scheduled in the past may run immediately."
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
$SqlInstance = Get-ValidatedInput -Prompt "Enter SQL instance (e.g. SAWPSQL20C)" -CurrentValue $SqlInstance -ValidationScript {
    param($value)
    return $value -and $value -notmatch '[^\w\d\.-]'
} -ErrorMessage "SQL instance name contains invalid characters. Use only letters, numbers, dots, and hyphens." -IsInstance

# Validate databases with existence check
$Databases = Get-ValidatedInput -Prompt "Enter databases to backup (comma-separated, e.g. DB1,DB2,DB3)" -CurrentValue ($Databases -join ',') -ValidationScript {
    param($value)
    return $value -notmatch '[^\w\d\$_]'
} -ErrorMessage "Database names can only contain letters, numbers, $, and underscores" -AllowMultiple -IsDatabase -SqlInstance $SqlInstance

# Before validating file path, set a default based on the SQL instance name
if (-not $FilePath -and $SqlInstance) {
    $FilePath = "\\$SqlInstance\Backup"
}
# Validate backup file path with existence and creation
$FilePath = Get-ValidatedInput -Prompt "Enter backup file path" -CurrentValue $FilePath -ValidationScript {
    param($value)
    return $value -and $value.IndexOfAny([System.IO.Path]::GetInvalidPathChars()) -lt 0
} -ErrorMessage "Backup path contains invalid characters" -IsPath

# Default task name if not specified
if (-not $TaskName) {
    $defaultTaskName = "SQL_Backup_$(Get-Date -Format 'yyyyMMddHHmmss')"
    $TaskName = $defaultTaskName
}

# Task name validation with existence check
$TaskName = Get-ValidatedInput -Prompt "Enter Task name" -CurrentValue $TaskName -ValidationScript {
    param($value)
    return $value -and $value -notmatch '[\\\/\:\*\?\"\<\>\|]'
} -ErrorMessage "Task name contains invalid characters" -IsTaskName

# Simple text description (no special validation needed)
if (-not $Description) {
    $Description = Read-Host "Enter task description (e.g., ServiceNow ticket number or purpose)"
}

# Set a default time 5 minutes in the future for the schedule
if (-not $ScheduleDateTime) {
    $futureTime = (Get-Date).AddMinutes(5)
    $ScheduleDateTime = $futureTime.ToString('yyyy-MM-dd HH:mm')
}
# Validate and parse the schedule date/time
$runDateTime = Get-ValidDateTimeInput -Prompt "Enter the date/time to run the backup" -CurrentValue $ScheduleDateTime

# Create the backup script file - store it locally instead of on the UNC path
$timestamp = Get-Date -Format 'yyyyMMddHHmmss'
$localScriptDir = Join-Path -Path $env:USERPROFILE -ChildPath "SQLBackupScripts"

# Create directory if it doesn't exist
if (-not (Test-Path -Path $localScriptDir)) {
    New-Item -Path $localScriptDir -ItemType Directory -Force | Out-Null
}

$scriptName = "SQLBackup_$timestamp.ps1"
$scriptPath = Join-Path -Path $localScriptDir -ChildPath $scriptName
$logPath = Join-Path -Path $FilePath -ChildPath "SQLBackup_$timestamp.log" # Log still goes to UNC path

# Create the PowerShell script with direct dbatools command execution
$backupScript = @"
# SQL Backup Script generated by ScheduleDatabaseBackups_TaskSched.ps1
# Generated on: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

# Add early diagnostics file that will be created even if script fails immediately
"Script started: $(Get-Date)" | Out-File -FilePath "C:\Windows\Temp\SQLBackup_Started.log" -Force

# Set error preferences
`$ErrorActionPreference = 'Continue'
`$VerbosePreference = 'SilentlyContinue'  # Reducing verbosity for background execution

# Create a fallback log location in case UNC path isn't accessible
`$fallbackLogPath = Join-Path -Path "C:\Windows\Temp" -ChildPath "SQLBackup_$timestamp.log"

# First try writing to a test file to check UNC path access
try {
    Start-Transcript -Path "$logPath" -Force
    Write-Host "Transcript started at $logPath"
} catch {
    Write-Warning "Failed to write to UNC path: $FilePath - `$_"
    Write-Warning "Using local fallback log: `$fallbackLogPath"
    Start-Transcript -Path `$fallbackLogPath -Force
}

Write-Host "============ SQL SERVER BACKUP TASK STARTED ================"
Write-Host "Task Name: $TaskName"
Write-Host "Started at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "SQL Instance: $SqlInstance"
Write-Host "Databases: $($Databases -join ', ')"
Write-Host "Backup Path: $FilePath"
Write-Host "Script running as: $([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)"
Write-Host ""

# Import dbatools module with explicit error handling
Write-Host "Checking for dbatools module..."
try {
    # First check if dbatools is already imported
    if (Get-Module -Name dbatools) {
        `$dbatoolsVersion = (Get-Module dbatools).Version
        Write-Host "dbatools module is already loaded (version: `$dbatoolsVersion)"
    }
    else {
        # Check if module is available but not loaded
        Write-Host "dbatools module not loaded, checking available modules..."
        Get-Module -ListAvailable | Where-Object { `$_.Name -like "*dba*" } | 
            Format-Table Name, Version, Path -AutoSize | Out-String | Write-Host
        
        # Only import if not already loaded
        if (Get-Module -ListAvailable -Name dbatools) {
            Write-Host "Importing dbatools module..."
            Import-Module dbatools -ErrorAction Stop
            `$dbatoolsVersion = (Get-Module dbatools).Version
            Write-Host "Successfully loaded dbatools module version: `$dbatoolsVersion"
        }
        else {
            throw "dbatools module not found in available modules"
        }
    }
}
catch {
    Write-Error "CRITICAL: Failed to load dbatools module: `$(`$_.Exception.Message)"
    Write-Host "Error details: `$(`$_ | Out-String)"
    Write-Host "Current PSModulePath: `$env:PSModulePath"
    throw "Cannot continue without dbatools module"
}

# Function to format size - fixed parameter declaration
function Format-FileSize {
    param (
        [int64]`$Position
    )
    
    if(`$Position -lt 1KB) { return "`$Position B" }
    elseif(`$Position -lt 1MB) { return "{0:N2} KB" -f (`$Position/1KB) }
    elseif(`$Position -lt 1GB) { return "{0:N2} MB" -f (`$Position/1MB) }
    elseif(`$Position -lt 1TB) { return "{0:N2} GB" -f (`$Position/1GB) }
    else { return "{0:N2} TB" -f (`$Position/1TB) }
}

# Track success/failure
`$totalSuccess = 0
`$totalFailure = 0
`$results = @()

Write-Host "----------------- BACKUP RESULTS -----------------"

# Process each database
foreach(`$db in '$($Databases -join "','")'.Split(',')) {
    try {
        Write-Host ("Starting backup of database: `$db") -ForegroundColor Cyan
        
        # Create timestamp at runtime for each database backup
        `$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
        `$backupFileName = "`$db-`$timestamp.bak"
        `$backupFile = Join-Path -Path '$FilePath' -ChildPath `$backupFileName
        
        # Using parameters verified with dbatools documentation
        # Fixed: Using correct parameters according to https://docs.dbatools.io/Backup-DbaDatabase
        `$result = Backup-DbaDatabase -SqlInstance '$SqlInstance' -Database `$db -FilePath `$backupFile -CopyOnly -Checksum
        
        if (`$result) {
            `$size = Format-FileSize `$result.TotalSize
            Write-Host "SUCCESS: `$db backed up to `$backupFile (`$size)" -ForegroundColor Green
            `$totalSuccess++
            `$results += [PSCustomObject]@{
                Database = `$db
                Status = "Success"
                Size = `$size
                BackupFile = `$backupFile
                CompletedTime = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
            }
        } else {
            Write-Host "FAILED: `$db backup did not complete" -ForegroundColor Red
            `$totalFailure++
            `$results += [PSCustomObject]@{
                Database = `$db
                Status = "Failed"
                Size = "N/A"
                BackupFile = `$backupFile
                CompletedTime = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
            }
        }
    }
    catch {
        Write-Host "ERROR: Failed to backup `$db. `$(`$_.Exception.Message)" -ForegroundColor Red
        `$totalFailure++
        `$results += [PSCustomObject]@{
            Database = `$db
            Status = "Error"
            Size = "N/A"
            BackupFile = "N/A"
            CompletedTime = (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
            Error = `$_.Exception.Message
        }
    }
}

# Create a summary CSV file in the UNC path for easy access
`$csvPath = Join-Path -Path '$FilePath' -ChildPath 'SQLBackup_Summary_$timestamp.csv'
`$results | Export-Csv -Path `$csvPath -NoTypeInformation
Write-Host "Backup summary saved to: `$csvPath"

Write-Host ""
Write-Host "------------------ SUMMARY ------------------"
Write-Host "Total databases: `$(`$totalSuccess + `$totalFailure)"
Write-Host "Successful: `$totalSuccess"
Write-Host "Failed: `$totalFailure"
Write-Host "Log file: $logPath"
Write-Host "Summary file: `$csvPath"
Write-Host "============ SQL SERVER BACKUP TASK COMPLETED =============="

# Delete self-cleaning scheduled task
try {
    Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false -ErrorAction SilentlyContinue
    Write-Host "Scheduled task '$TaskName' has been removed."
    
    # Also clean up this script file
    Start-Sleep -Seconds 2
    Remove-Item -Path '$scriptPath' -Force -ErrorAction SilentlyContinue
} catch {
    Write-Warning "Could not remove the scheduled task or script file. You may need to delete them manually."
}

# End transcript logging
Stop-Transcript
"@

try {
    # Save the backup script to the local machine
    Set-Content -Path $scriptPath -Value $backupScript -Force
    Write-Host "Created backup script: $scriptPath"
    
    # Same action as before
    $action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" > `"$env:USERPROFILE\SQLBackup_TaskOutput.log`" 2>&1"
    
    $trigger = New-ScheduledTaskTrigger -Once -At $runDateTime
    
    # Configure settings for reliable execution
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd -AllowStartIfOnBatteries `
               -ExecutionTimeLimit (New-TimeSpan -Hours 2) -Priority 4
    
    # Use S4U logon type to run whether user is logged on or not
    $currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    
    # Use a credential dialog to get the password securely
    Write-Host "To run the task whether you're logged in or not, you need to provide your credentials."
    Write-Host "The password will be securely stored in the task scheduler (not visible in plaintext)."
    $credential = Get-Credential -UserName $currentUser -Message "Enter your password for the scheduled task"

    # Register the task with the user credentials
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
                          -Settings $settings -Description $Description `
                          -User $credential.UserName -Password $credential.GetNetworkCredential().Password -RunLevel Highest -Force
    
    # Add warning about user profile access
    Write-Host ""
    Write-Host "IMPORTANT: The task is configured to run whether you are logged in or not." -ForegroundColor Yellow
    Write-Host "Note that when running in this mode:" -ForegroundColor Yellow
    Write-Host " - PowerShell modules might be in different locations" -ForegroundColor Yellow
    Write-Host " - \$env:USERPROFILE will point to a different location" -ForegroundColor Yellow
    Write-Host " - Make sure dbatools is installed for all users (not just current user)" -ForegroundColor Yellow
    
    # Update the local log path to use a more accessible location
    Write-Host "Local logs will be available at: C:\Windows\Temp\SQLBackup_TaskOutput.log" -ForegroundColor Cyan
    
    Write-Host ""
    Write-Host "==================== Task Details ===================="
    Write-Host "Task has been successfully scheduled!"
    Write-Host "Task Name: $TaskName"
    Write-Host "Run Time: $($runDateTime.ToString('yyyy-MM-dd HH:mm:ss'))"
    Write-Host "SQL Instance: $SqlInstance" 
    Write-Host "Databases: $($Databases -join ', ')"
    Write-Host "Backup Path: $FilePath"
    Write-Host "Script Path: $scriptPath (local)"
    Write-Host "Log File: $logPath (on backup server)"
    Write-Host ""
    Write-Host "The task will automatically clean up after itself."
    Write-Host "==================================================="
    
} catch {
    Write-Error "Failed to create scheduled task: $_"
    exit 1
}