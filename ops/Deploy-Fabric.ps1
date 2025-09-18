param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,

  # You may provide either PipelineId OR PipelineName. Name will be used if Id is empty.
  [string]$PipelineId = "",
  [Parameter(Mandatory=$false)][string]$PipelineName = "",

  [Alias('SourceStageName')]
  [string]$SourceStage = "Development",

  [Alias('TargetStageName')]
  [string]$TargetStage = "Production",

  [string]$Note = "CI/CD deploy via GitHub Actions",


  [string]$ItemsJson = "",

  # Default-exclude Warehouses to avoid PrincipalTypeNotSupported with SPNs
  [string[]]$ExcludeItemTypes = @("Warehouse")
)

$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

function Get-FabricToken {
  param([string]$TenantId,[string]$ClientId,[string]$ClientSecret)
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }
  (Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" `
    -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token = Get-FabricToken $TenantId $ClientId $ClientSecret
$authH = @{ Authorization = "Bearer $token" }

function GetJson($uri) {
  Invoke-RestMethod -Headers $authH -Uri $uri -Method GET
}

function PostLro($uri, $obj) {
  $json = $obj | ConvertTo-Json -Depth 20
  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers ($authH + @{ "Content-Type"="application/json" }) -Body $json -MaximumRedirection 0

  # LRO pattern
  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) { $opUrl = $resp.Headers["operation-location"] }
  if (-not $opUrl) { $opUrl = $resp.Headers["Location"] }

  if ($resp.StatusCode -eq 202 -and $opUrl) {
    do {
      Start-Sleep -Seconds 5
      $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $authH
      $pct = if ($op.PSObject.Properties.Name -contains "percentComplete") { $op.percentComplete } else { "" }
      Write-Host ("Deployment status: {0} {1}" -f $op.status, (if($pct -ne ""){"($pct%)"}else{""}))
    } while ($op.status -in @("NotStarted","Running"))

    if ($op.status -ne "Succeeded") {
      try {
        $res = Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $authH
        throw "Deployment failed. Status: $($op.status). Details:`n$(($res | ConvertTo-Json -Depth 20))"
      } catch {
        throw "Deployment failed. Status: $($op.status)."
      }
    }

    try {
      return Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $authH
    } catch {
      return $op
    }
  }
  elseif ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
    if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) } else { return $null }
  }
  else {
    throw "Unexpected response: HTTP $($resp.StatusCode) $($resp.StatusDescription) — $($resp.Content)"
  }
}

# ---- Resolve pipeline ----
$pipeId = $PipelineId
if (-not $pipeId) {
  $all = (GetJson "$base/deploymentPipelines").value
  if (-not $all) { throw "No deployment pipelines visible to the service principal." }
  $targetName = ($PipelineName ?? "").Trim()
  $pipe = $all | Where-Object { (($_.displayName ?? "")).Trim() -ieq $targetName }
  if (-not $pipe) {
    Write-Host "Pipelines visible to SPN:"
    foreach($p in $all){ Write-Host " - $($p.displayName) [$($p.id)]" }
    throw "Deployment pipeline '$PipelineName' not found."
  }
  $pipeId = $pipe.id
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
} else {
  $pipe = GetJson "$base/deploymentPipelines/$pipeId"
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
}

# ---- Resolve stages by NAME ----
$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages").value
$src = $stages | Where-Object { (($_.displayName ?? "").Trim()) -ieq $SourceStage.Trim() }
$dst = $stages | Where-Object { (($_.displayName ?? "").Trim()) -ieq $TargetStage.Trim() }
if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline '$($pipe.displayName)'." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline '$($pipe.displayName)'." }
Write-Host "Source stage: $($src.displayName) [$($src.id)]"
Write-Host "Target stage: $($dst.displayName) [$($dst.id)]"

# ---- Build request body ----
$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
}

function Filter-Excluded($items, $exclude){
  $kept = @()
  foreach($i in $items){
    if ($exclude -contains $i.itemType) {
      Write-Host "Skipping item due to ExcludeItemTypes: $($i.itemDisplayName ?? $i.sourceItemId) [$($i.itemType)]"
      continue
    }
    $kept += $i
  }
  return $kept
}

# ---- Build items list ----
if ($ItemsJson -and $ItemsJson.Trim()){
  $wanted = $ItemsJson | ConvertFrom-Json
  $stageItems = (GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items").value

  $itemsToSend = @()
  foreach($w in $wanted){
    if ($w.PSObject.Properties.Name -contains "sourceItemId" -and $w.sourceItemId -match '^[0-9a-fA-F-]{36}$'){
      $itemsToSend += @{ sourceItemId = $w.sourceItemId; itemType = $w.itemType; itemDisplayName = $w.itemDisplayName }
    } else {
      if (-not ($w.PSObject.Properties.Name -contains "itemDisplayName")) {
        throw "Each item must have 'sourceItemId' or 'itemDisplayName'. Offending entry: $($w | ConvertTo-Json -Compress)"
      }
      $match = $stageItems | Where-Object { $_.itemDisplayName -eq $w.itemDisplayName -and $_.itemType -eq $w.itemType }
      if (-not $match) { throw "Item not found in source stage: '$($w.itemDisplayName)' [$($w.itemType)]." }
      $itemsToSend += @{ sourceItemId = $match.sourceItemId; itemType = $match.itemType; itemDisplayName = $match.itemDisplayName }
    }
  }

  $itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes
  if(-not $itemsToSend){ throw "All requested items were excluded. Nothing to deploy." }

  Write-Host "Items to deploy:"
  foreach($i in $itemsToSend){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)]" }

  # Strip display name for API body
  $body.items = $itemsToSend | ForEach-Object { @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType } }
}
else {
  # Deploy “everything” from source stage minus excluded types
  $stageItems = (GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items").value
  $itemsToSend = $stageItems | ForEach-Object {
    @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType; itemDisplayName = $_.itemDisplayName }
  }
  $itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes
  if(-not $itemsToSend){ throw "After exclusions, no items remain to deploy." }

  Write-Host "Items to deploy:"
  foreach($i in $itemsToSend){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)]" }

  $body.items = $itemsToSend | ForEach-Object { @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType } }
}

# ---- Kick off deployment ----
$deployUri = "$base/deploymentPipelines/$pipeId/deploy"
$result = PostLro $deployUri $body

Write-Host "✅ Deployment Succeeded."
if ($result) { $result | ConvertTo-Json -Depth 20 }
