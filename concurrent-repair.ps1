[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)][string]$spn,         # spn for an id that can create and modify VMs, create RGs, and public IPs.  E.g. "Contributor"
    [Parameter(Mandatory = $true)][string]$passphrase,  # passphrase for the SPN
    [Parameter(Mandatory = $true)][string]$tenant       # your tenant ID
)

$ErrorActionPreference = 'Continue'

$maxConcurrentJobs = 20             # how many concurrent jobs?
$patientsCsvPath = "vms.csv"        # name of input CSV

<#
# format for input CSV - Columns as follows:
# Sub, RgName, VmName
# where... Sub is subscription name, RgName is the resource group containing affected VM, VmName is the name of the patient VM to repair

Example of launch.json

{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "PowerShell: Launch Current File",
            "type": "PowerShell",
            "request": "launch",
            "script": "${file}",
            "args": ["-spn 'your SPN/ app reg ID'", "-passphrase 'your passphrase'", "-tenant 'your tenant ID'" ]
        }
    ]
}
#>

# import patients from CSV
$patients = Get-Content -Path $patientsCsvPath | ConvertFrom-Csv | Where-Object { $_.SubscriptionName -ne ""}

<#
OR, enumerate patients with a hand-made list.. 
$patients = @(
    [pscustomobject]@{
        Sub    = "some sub"
        RgName = "some rg"
        VmName = "some vm name"
    },
    [pscustomobject]@{
        Sub    = "some sub"
        RgName = "some rg"
        VmName = "some other vm name"
    }
)
#>

$fixPatient = {
    param ($patient)

    # a random string to use to avoid namespace collisions
    $tmpSuffix = -join ((97..122) | Get-Random -Count 5 | ForEach-Object { [char]$_ })
    $repair = @{
        Id     = $null
        SubId  = $patient.subId
        RgName = "jmd-repair-$tmpSuffix-rg"
        VmName = "repair$tmpSuffix"
    }

    # verify that this patient needs work - skip off vms
    $powerState = az vm show -g $patient.RgName -n $patient.VmName -d --query powerState | ConvertFrom-Json
    if (-not $powerState -eq "VM running") {
        Write-Output "Patient $($patient.VmName) not running, skipping"
        return
    }

    # verify that this patient needs work - skip vms with read vmAgents
    $agentStatus = az vm get-instance-view -g $patient.RgName -n $patient.VmName  --query instanceView.vmAgent.statuses[0].displayStatus | ConvertFrom-Json
    if ($agentStatus -eq "Ready") {
        # vm is already happy
        Write-Output "Patient $($patient.VmName) seems happy, skipping"
        return 
    }

    # ok it needs repair
    Write-Output "Patient $($patient.VmName) is borked, using suffix $tmpSuffix during repair"


    ##### SWAP OS DISK BACK TO ORIGINAL
    Write-Output "Checking $($patient.VmName) disk - is it original, or copy?"
    $currentOsDiskName = (az vm show --resource-group $patient.RgName --name $patient.VmName | ConvertFrom-Json).storageProfile.osDisk.name

    if ($currentOsDiskName -like "*-DiskCopy-*") {
        Write-Output "Attempting to swap patient's DiskCopy OS disk back to original OS disk"
        
        # find the original OS disk name
        $orignalOsDisk = az disk list --resource-group $patient.RgName | ConvertFrom-Json | Where-Object { $_.name -like "$($patient.VmName)-osdisk" }
        if (-not $orignalOsDisk) {
            Write-Output "Could not find original OS disk for $($patient.VmName)"
            return
        }

        # find the original OS disk name
        if (-not $orignalOsDisk) {
            Write-Output "Could not find original OS disk for $($patient.VmName)"
            return
        }

        # Deallocate the VM
        Write-Output "Deallocating $($patient.VmName)"
        $tmpDeallocate = az vm deallocate --resource-group $patient.RgName --name $patient.VmName

        # confirm it's not attached elsewhere (if it is, we need to abort)
        if ($orignalOsDisk.diskState -eq "Attached") {
            Write-Output "OS disk $($orignalOsDisk.Name) is already attached to something else, cannot process $($patient.VmName)"

            # Start the VM
            Write-Output "Re-starting $($patient.VmName)"
            $tmpStart = az vm start --resource-group $patient.RgName --name $patient.VmName 
            
            return
        }

        # Update the VM to use the new OS disk
        Write-Output "Swapping $($patient.VmName) OS disk"
        $tmpSwap = az vm update --resource-group $patient.RgName --name $patient.VmName --os-disk $orignalOsDisk.Id

        # Start the VM with no-wait, since VM Agent won't come up
        Write-Output "Re-starting $($patient.VmName)"
        $tmpStart = az vm start --resource-group $patient.RgName --name $patient.VmName --no-wait
        Start-Sleep -seconds 30
        
        Write-Output "Done swapping OS disks"
    }
    
    # create the repair vm.  bug with creating public ip, but whatever it works
    Write-Output "Creating repair VM $($repair.VmName) in $($repair.RgName)"
    az vm repair create  --name $patient.VmName --resource-group $patient.RgName --repair-group-name $repair.RgName --repair-vm-name $repair.VmName --repair-username 'adminuser' --repair-password 'crowdstrike123yousuck!' --unlock-encrypted-vm --yes

    # repair vm's id
    $repair.Id = "/subscriptions/$($repair.subId)/resourceGroups/$($repair.RgName)/providers/Microsoft.Compute/virtualMachines/$($repair.VmName)"
    Write-Output "Repair VM ID is $($repair.Id)"

    # run the repair script
    Write-Output "Running repair on VM $($repair.VmName) in $($repair.RgName), against $($patient.VmName) in $($patient.RgName)"
    az vm repair run     --name $patient.VmName --resource-group $patient.RgName --repair-vm-id $repair.Id --run-id win-crowdstrike-fix-bootloop-v2

    # give the patient the repaired disk, destroy repair resources
    Write-Output "Restoring repaired disk on $($patient.VmName) in $($patient.RgName)"
    az vm repair restore --name $patient.VmName --resource-group $patient.RgName --repair-vm-id $repair.Id --yes

    Write-Output "Done processing patient $($patient.VmName)"
    return
}


$jobs = @()
Write-Output "Working on the following patients:"
$patients | Write-Output
Write-Output ""

foreach ($subName in $patients.Sub | Sort-Object -unique ) {
    Write-Output "Processing patients in $subName"

    # login again, keep shit fresh
    $login = az login --service-principal -u $spn -p $passphrase --tenant $tenant

    # select sub by name and get its id for later
    $acct = az account set --name $subName
    $subId = (az account show | ConvertFrom-Json).Id

    Write-Output "Running up to $maxConcurrentJobs concurrent jobs against patients in $subName"

    foreach ($patient in $patients | where-object { $_.Sub -eq $subName }) {

        $patient | Add-Member -MemberType NoteProperty -Name subId -Value $subId -force

        # Wait if there are already 15 running jobs
        while ($jobs.Count -ge $maxConcurrentJobs) {
            $completedJobs = $jobs | Where-Object { $_.State -eq 'Completed' }
            foreach ($completedJob in $completedJobs) {
                $jobs = $jobs.Where({ $_.Id -ne $completedJob.Id })

                Receive-Job -Job $completedJob
                Remove-Job -Job $completedJob
            }
            Start-Sleep -Seconds 1
        }
        
        # sometimes $jobs needs to be reminded it's an array.. 
        if ((-not $jobs) -and (-not $jobs -is [array])) {
            $jobs = @()
        }

        # kick off a repair
        $jobs += Start-Job -ScriptBlock $fixPatient -ArgumentList $patient
    }

    # Continue checking for and processing completed jobs, and deal with any hung jobs.. 
    $i = 0
    while ($jobs.Count -gt 0) {
        $i++
        if ($i % 10 -eq 0) {
            Write-Output "Waiting for $($jobs.count) background jobs before we can move to next sub"
        }

        # abort jobs that have taken way too long, so they don't prevent us from moving to the next sub
        $now = Get-Date
        $longRunningJobs = $jobs | Where-Object { ($_.State -ne 'Completed') -and (($now - $_.PSBeginTime).TotalMinutes -ge 70) }
        foreach ($longRunningJob in $longRunningJobs) {
            Write-Output "Job $($longRunningJob.Id) has been running for more than 45 mins.. aborting it"
            $jobs = $jobs.Where({ $_.Id -ne $longRunningJob.Id })
            
            Stop-Job -Confirm -ErrorAction SilentlyContinue -Job $longRunningJob
            Receive-Job -Job $longRunningJob
            Remove-Job -Confirm -ErrorAction SilentlyContinue -Job $longRunningJob
        }
        
        # handle completed jobs
        $completedJobs = $jobs | Where-Object { $_.State -eq 'Completed' }
        foreach ($completedJob in $completedJobs) {
            $jobs = $jobs.Where({ $_.Id -ne $completedJob.Id })

            Receive-Job -Job $completedJob
            Remove-Job -Job $completedJob
        }

        Start-Sleep -Seconds 1
    }
    Write-Output "Moving to next sub, let's go!"
    Write-Output ""
}
