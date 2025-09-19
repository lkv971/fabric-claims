<# 
Deploy Dev → Prod for a Fabric deployment pipeline using stage DISPLAY NAMES.
- Resolves pipeline & stages by NAME (no GUIDs needed).
- Skips any stage items that lack a valid sourceItemId (GUID).
- Default excludes: Warehouse, VariableLibrary (tweak ExcludeItemTypes as needed).
- Optional -ItemsJson for selective deploy (by displayName or sourceItemId).
- Polls the Long Running Operation (LRO) until Succeeded and prints details if Failed.
#>

param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,

  # Provide either PipelineId OR PipelineName (name used if Id is empty)
  [string]$PipelineId = "",
  [Parameter(Mandatory=$false)][string]$PipelineName = "",

  [Alias('SourceStageName')]
  [string]$SourceStage = "Development",

  [Alias('TargetStageName')]
  [string]$TargetStage = "Production",

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional JSON array for selective deploy.
  # Example:
  # [
  #   {"itemDisplayName":"PLclaims_master","itemType":"DataPipeline"},
  #   {"itemDisplayName":"SM_Claims","itemType":"SemanticModel"},
  #   {"itemDisplayName":"Health","itemType":"Report"}
  # ]
  [string]$ItemsJson = "",

  # Default exclusions (change if you want to deploy these too)
  [string[]]$ExcludeItemTypes = @("Warehouse","VariableLibrary")
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
  $tokenUri = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  Write-Host "DEBUG: Token URI type: $($tokenUri.GetType().FullName) value: $tokenUri"
  (Invoke-RestMethod -Method POST -Uri $tokenUri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token = Get-FabricToken $TenantId $ClientId $ClientSecret
$Headers = @{ Authorization = "Bearer $token" }  # single Hashtable (important)

function GetJson($uri) {
  if ($uri -is [array]) { $uri = $uri[0] }
  Write-Host "DEBUG: GET URI type: $($uri.GetType().FullName) value: $uri"
  Invoke-RestMethod -Headers $Headers -Uri $uri -Method GET
}

function PostLro($uri, $obj) {
  if ($uri -is [array]) { $uri = $uri[0] }
  Write-Host "DEBUG: POST URI type: $($uri.GetType().FullName) value: $uri"
  Write-Host "DEBUG: Headers type: $($Headers.GetType().FullName)"
  $json = $obj | ConvertTo-Json -Depth 20

  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers $Headers -ContentType "application/json" -Body $json -MaximumRedirection 0

  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) { $opUrl = $resp.Headers["operation-location"] }
  if (-not $opUrl) { $opUrl = $resp.Headers["Location"] }

  if ($resp.StatusCode -eq 202 -and $opUrl) {
    if ($opUrl -is [array]) { $opUrl = $opUrl[0] }
    Write-Host "DEBUG: LRO URI type: $($opUrl.GetType().FullName) value: $opUrl"

    do {
      Start-Sleep -Seconds 5
      $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $Headers
      $pct = if ($op.PSObject.Properties.Name -contains "percentComplete") { $op.percentComplete } else { "" }
      Write-Host ("Deployment status: {0} {1}" -f $op.status, ($pct ? "($pct%)" : ""))
    } while ($op.status -in @("NotStarted","Running"))

    if ($op.status -ne "Succeeded") {
      # Try to print extended result
      try {
        $res = Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $Headers
        $msgs = @("Deployment failed. Status: $($op.status).")
        if ($res) {
          $msgs += "Extended result:"
          $msgs += ($res | ConvertTo-Json -Depth 20)
        }
        throw ($msgs -join "`n")
      } catch {
        throw "Deployment failed. Status: $($op.status)."
      }
    }

    try {
      return Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $Headers
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

# ---- Load source stage items; keep only deployable ones (valid GUID) ----
$itemsUri = "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items"
$stageItems = (GetJson $itemsUri).value

# Filter out items missing sourceItemId GUID (Fabric can list objects that aren’t deployable yet)
$stageItems = $stageItems | Where-Object {
  $_.PSObject.Properties.Name -contains "sourceItemId" -and
  $_.sourceItemId -and
  ($_.sourceItemId -match '^[0-9a-fA-F-]{36}$')
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
$itemsToSend = @()

if ($ItemsJson -and $ItemsJson.Trim()){
  $wanted = $ItemsJson | ConvertFrom-Json
  foreach($w in $wanted){
    if ($w.PSObject.Properties.Name -contains "sourceItemId" -and $w.sourceItemId -match '^[0-9a-fA-F-]{36}$'){
      $itemsToSend += @{
        sourceItemId     = $w.sourceItemId
        itemType         = $w.itemType
        itemDisplayName  = $w.itemDisplayName
      }
    } else {
      if (-not ($w.PSObject.Properties.Name -contains "itemDisplayName")) {
        throw "Each item must have 'sourceItemId' or 'itemDisplayName'. Offending: $($w | ConvertTo-Json -Compress)"
      }
      $match = $stageItems | Where-Object {
        $_.itemDisplayName -eq $w.itemDisplayName -and $_.itemType -eq $w.itemType
      }
      if (-not $match) {
        throw "Item not found or not deployable from source stage: '$($w.itemDisplayName)' [$($w.itemType)]."
      }
      $itemsToSend += @{
        sourceItemId     = $match.sourceItemId
        itemType         = $match.itemType
        itemDisplayName  = $match.itemDisplayName
      }
    }
  }
}
else {
  # Deploy “everything” that is deployable (has sourceItemId), minus excludes
  $itemsToSend = $stageItems | ForEach-Object {
    @{
      sourceItemId     = $_.sourceItemId
      itemType         = $_.itemType
      itemDisplayName  = $_.itemDisplayName
    }
  }
}

# Apply exclusions (Warehouse, VariableLibrary by default)
$itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes

if (-not $itemsToSend) {
  throw "After filtering items without sourceItemId and applying exclusions, no items remain to deploy."
}

Write-Host "Items to deploy:"
foreach($i in $itemsToSend){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)]" }

# Build request body
$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
  items = ($itemsToSend | ForEach-Object { @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType } })
}

# Kick off deployment
$deployUri = "$base/deploymentPipelines/$pipeId/deploy"
Write-Host "Deploy URI: $deployUri"
$result = PostLro $deployUri $body

Write-Host "Deployment Succeeded."
if ($result) { $result | ConvertTo-Json -Depth 20 }
