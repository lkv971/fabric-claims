<#
Deploy Dev → Prod using Fabric Deployment Pipeline REST API (stage NAMES).

Hardening:
- Ensure all URIs are strings and log them.
- Throw on ambiguous pipeline/stage names.
- Coalesce item ids (sourceItemId/itemId) and filter nulls.
- Default-exclude Warehouse, VariableLibrary, DataPipelineTrigger.
- When LRO fails, fetch and print detailed error from multiple places.
#>

param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,

  # Provide PipelineId OR PipelineName (name used if id empty).
  [string]$PipelineId = "",
  [Parameter(Mandatory=$false)][string]$PipelineName = "",

  [Alias('SourceStageName')]
  [string]$SourceStage = "Development",

  [Alias('TargetStageName')]
  [string]$TargetStage = "Production",

  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional selective deploy JSON (array of objects with itemDisplayName/itemType or sourceItemId/itemType)
  [string]$ItemsJson = "",

  # Default excludes (override with -ExcludeItemTypes @() if needed)
  [string[]]$ExcludeItemTypes = @("Warehouse","VariableLibrary","DataPipelineTrigger")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$base = "https://api.fabric.microsoft.com/v1"

function Ensure-String([object]$x) {
  if ($null -eq $x) { return "" }
  if ($x -is [array]) { return [string]$x[0] }
  return [string]$x
}

function Get-FabricToken {
  param([string]$TenantId,[string]$ClientId,[string]$ClientSecret)
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }
  $uri = Ensure-String "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
  Write-Host "DEBUG: Token URI type: $($uri.GetType().FullName) value: $uri"
  (Invoke-RestMethod -Method POST -Uri $uri -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

$token = Get-FabricToken $TenantId $ClientId $ClientSecret
$authH = @{ Authorization = "Bearer $token"; Accept = "application/json" }

function New-JsonHeaders([hashtable]$baseHeaders) {
  $h = @{}
  foreach ($kv in $baseHeaders.GetEnumerator()) { $h[$kv.Key] = $kv.Value }
  $h["Content-Type"] = "application/json"
  return $h
}

function GetJson([string]$uri) {
  $uri = Ensure-String $uri
  Write-Host "DEBUG: GET URI type: $($uri.GetType().FullName) value: $uri"
  Invoke-RestMethod -Headers $authH -Uri $uri -Method GET
}

function TryGetRawString($uri) {
  try {
    $resp = Invoke-WebRequest -Headers $authH -Uri $uri -Method GET
    return $resp.Content
  } catch {
    try {
      # If there is a response body on error, surface it
      $er = $_.Exception
      if ($er.Response -and $er.Response.GetResponseStream) {
        $reader = New-Object System.IO.StreamReader($er.Response.GetResponseStream())
        return $reader.ReadToEnd()
      }
    } catch {}
    return ""
  }
}

function PostLro([string]$uri, [object]$obj, [string]$pipelineIdForAltQueries) {
  $uri     = Ensure-String $uri
  $headers = New-JsonHeaders $authH
  $json    = $obj | ConvertTo-Json -Depth 20

  Write-Host "DEBUG: POST URI type: $($uri.GetType().FullName) value: $uri"
  Write-Host "DEBUG: Headers type: $($headers.GetType().FullName)"

  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers $headers -Body $json -MaximumRedirection 0

  # LRO link (cluster domain)
  $opUrl = $resp.Headers["Operation-Location"]
  if (-not $opUrl) { $opUrl = $resp.Headers["operation-location"] }
  if (-not $opUrl) { $opUrl = $resp.Headers["Location"] }
  $opUrl = Ensure-String $opUrl
  if ($opUrl) { Write-Host "DEBUG: LRO URI type: $($opUrl.GetType().FullName) value: $opUrl" }

  if ($resp.StatusCode -eq 202 -and $opUrl) {
    do {
      Start-Sleep -Seconds 5
      $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $authH
      $pct = if ($op.PSObject.Properties.Name -contains "percentComplete") { $op.percentComplete } else { "" }
      Write-Host ("Deployment status: {0} {1}" -f $op.status, ($pct ? "($pct%)" : ""))
    } while ($op.status -in @("NotStarted","Running"))

    if ($op.status -ne "Succeeded") {
      # Try to fetch result/details from cluster LRO endpoint
      $clusterResult = TryGetRawString ($opUrl.TrimEnd('/') + "/result")

      # Also try API-side operations endpoints if we can extract an operation id
      $opId = $null
      if ($op.PSObject.Properties.Name -contains "id") { $opId = $op.id }
      if (-not $opId) {
        if ($opUrl -match "/operations/([0-9a-fA-F-]{36})") { $opId = $Matches[1] }
      }

      $apiOp = ""; $apiOpResult = ""
      if ($opId) {
        $apiOp       = TryGetRawString ("$base/deploymentPipelines/$pipelineIdForAltQueries/operations/$opId")
        $apiOpResult = TryGetRawString ("$base/deploymentPipelines/$pipelineIdForAltQueries/operations/$opId/result")
      }

      $msg = @()
      $msg += "Deployment failed. Status: $($op.status)."
      if ($clusterResult) { $msg += "Cluster /result payload:`n$clusterResult" }
      if ($apiOp)         { $msg += "API op state:`n$apiOp" }
      if ($apiOpResult)   { $msg += "API op result:`n$apiOpResult" }
      throw ($msg -join "`n`n")
    }

    try { return Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $authH }
    catch { return $op }
  }
  elseif ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 300) {
    if ($resp.Content) { return ($resp.Content | ConvertFrom-Json) } else { return $null }
  }
  else {
    throw "Unexpected response: HTTP $($resp.StatusCode) $($resp.StatusDescription) — $($resp.Content)"
  }
}

# --------- Resolve pipeline
$pipeId = $PipelineId
if (-not $pipeId) {
  $all = (GetJson "$base/deploymentPipelines").value
  if (-not $all) { throw "No deployment pipelines visible to the service principal." }
  $targetName = ($PipelineName ?? "").Trim()

  $matches = $all | Where-Object { (($_.displayName ?? "")).Trim() -ieq $targetName }
  if (-not $matches) {
    Write-Host "Pipelines visible to SPN:"
    foreach($p in $all){ Write-Host " - $($p.displayName) [$($p.id)]" }
    throw "Deployment pipeline '$PipelineName' not found."
  }
  if (@($matches).Count -gt 1) {
    foreach($m in $matches){ Write-Host "Duplicate pipeline: $($m.displayName) [$($m.id)]" }
    throw "Ambiguous pipeline name. Please pass -PipelineId."
  }

  $pipe   = $matches
  $pipeId = Ensure-String $pipe.id
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
} else {
  $pipe = GetJson "$base/deploymentPipelines/$pipeId"
  Write-Host "Pipeline: $($pipe.displayName) [$pipeId]"
}

# --------- Resolve stages by NAME
$stages = (GetJson "$base/deploymentPipelines/$pipeId/stages").value
$srcMatches = $stages | Where-Object { (($_.displayName ?? "").Trim()) -ieq $SourceStage.Trim() }
$dstMatches = $stages | Where-Object { (($_.displayName ?? "").Trim()) -ieq $TargetStage.Trim() }

if (-not $srcMatches) { throw "Source stage '$SourceStage' not found in pipeline '$($pipe.displayName)'." }
if (-not $dstMatches) { throw "Target stage '$TargetStage' not found in pipeline '$($pipe.displayName)'." }
if (@($srcMatches).Count -gt 1) { throw "Ambiguous SourceStage name. Make sure it's unique." }
if (@($dstMatches).Count -gt 1) { throw "Ambiguous TargetStage name. Make sure it's unique." }

$src = $srcMatches
$dst = $dstMatches

Write-Host "Source stage: $($src.displayName) [$($src.id)]"
Write-Host "Target stage: $($dst.displayName) [$($dst.id)]"

# --------- Build base body
$body = @{
  sourceStageId = Ensure-String $src.id
  targetStageId = Ensure-String $dst.id
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

# --------- Load items in source stage, coalesce ids
$raw = (GetJson "$base/deploymentPipelines/$pipeId/stages/$($src.id)/items").value

$stageItems = @()
foreach($it in $raw){
  $sid = $null
  if ($it.PSObject.Properties.Name -contains "sourceItemId" -and $it.sourceItemId) { $sid = $it.sourceItemId }
  elseif ($it.PSObject.Properties.Name -contains "itemId"      -and $it.itemId)      { $sid = $it.itemId }

  if ($sid -and ($sid -match '^[0-9a-fA-F-]{36}$')) {
    $stageItems += [pscustomobject]@{
      itemDisplayName = $it.itemDisplayName
      itemType        = $it.itemType
      sourceItemId    = $sid
    }
  } else {
    Write-Host "Skipping (no valid id): $($it.itemDisplayName) [$($it.itemType)]"
  }
}

if (-not $stageItems) {
  throw "No deployable items found in source stage after id coalescing."
}

# --------- Build items to send (selective or all)
$itemsToSend = @()
if ($ItemsJson -and $ItemsJson.Trim()){
  $wanted = $ItemsJson | ConvertFrom-Json
  foreach($w in $wanted){
    if ($w.PSObject.Properties.Name -contains "sourceItemId" -and $w.sourceItemId -match '^[0-9a-fA-F-]{36}$'){
      $itemsToSend += @{
        sourceItemId     = $w.sourceItemId
        itemType         = $w.itemType
        itemDisplayName  = ($w.itemDisplayName ?? $w.sourceItemId)
      }
    } else {
      if (-not ($w.PSObject.Properties.Name -contains "itemDisplayName")) {
        throw "Each item must have 'sourceItemId' or 'itemDisplayName'. Offending: $($w | ConvertTo-Json -Compress)"
      }
      $match = $stageItems | Where-Object {
        $_.itemDisplayName -eq $w.itemDisplayName -and $_.itemType -eq $w.itemType
      }
      if (-not $match) {
        throw "Item not found in source stage: '$($w.itemDisplayName)' [$($w.itemType)]."
      }
      $itemsToSend += @{
        sourceItemId     = $match.sourceItemId
        itemType         = $match.itemType
        itemDisplayName  = $match.itemDisplayName
      }
    }
  }
} else {
  $itemsToSend = $stageItems
}

# Exclude unwanted types (Warehouse, VariableLibrary, Triggers) AFTER id filtering
$itemsToSend = Filter-Excluded $itemsToSend $ExcludeItemTypes
if (-not $itemsToSend) { throw "After exclusions, no items remain to deploy." }

Write-Host "Items to deploy:"
foreach($i in $itemsToSend){ Write-Host " - $($i.itemDisplayName) [$($i.itemType)]" }

# API body expects only id + type
$body.items = $itemsToSend | ForEach-Object { @{ sourceItemId = $_.sourceItemId; itemType = $_.itemType } }

# --------- Kick off deployment
$deployUri = [string]::Concat($base, "/deploymentPipelines/", (Ensure-String $pipeId), "/deploy")
Write-Host "Deploy URI: $deployUri"
$result = PostLro $deployUri $body $pipeId

Write-Host "✅ Deployment Succeeded."
if ($result) { $result | ConvertTo-Json -Depth 20 }
