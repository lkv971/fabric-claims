Param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,

  [Parameter(Mandatory=$true)][string]$PipelineName,
  [string]$SourceStage = "Development",
  [string]$TargetStage = "Production",
  [string]$Note = "CI/CD deploy via GitHub Actions",

  # Optional selective deploy list. Either:
  #   [{"itemDisplayName":"PLclaims_master","itemType":"DataPipeline"}, ...]
  # OR
  #   [{"sourceItemId":"<GUID>","itemType":"DataPipeline"}, ...]
  [string]$ItemsJson = ""
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
  $resp = Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $body
  return $resp.access_token
}

$token = Get-FabricToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
$authH = @{ Authorization = "Bearer $token" }

function GetJson($uri) {
  (Invoke-RestMethod -Method GET -Uri $uri -Headers $authH)
}

function PostLro($uri, $bodyObj) {
  $json = $bodyObj | ConvertTo-Json -Depth 10
  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers ($authH + @{ "Content-Type"="application/json" }) -Body $json -MaximumRedirection 0 -ErrorAction Stop
  if ($resp.StatusCode -eq 202) {
    $opUrl = $resp.Headers["Location"]
    if (-not $opUrl) { throw "202 Accepted but no Location header returned." }

    do {
      Start-Sleep -Seconds 5
      $op = Invoke-RestMethod -Method GET -Uri $opUrl -Headers $authH
      Write-Host "Deployment status: $($op.status) ($($op.percentComplete)%)"
    } while ($op.status -eq "Running" -or $op.status -eq "NotStarted")

    if ($op.status -ne "Succeeded") {
      # Try to fetch result for error details
      try {
        $res = Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $authH
        throw "Deployment failed. Status: $($op.status). Details: $(($res | ConvertTo-Json -Depth 10))"
      } catch {
        throw "Deployment failed. Status: $($op.status)."
      }
    }

    # Success – try to grab the result doc if available
    try {
      return Invoke-RestMethod -Method GET -Uri ($opUrl.TrimEnd('/') + "/result") -Headers $authH
    } catch {
      return $op
    }
  } else {
    # Some endpoints return 200 with a payload
    return $resp.Content | ConvertFrom-Json
  }
}

# 1) Find the deployment pipeline by name
$pipes = GetJson "$base/deploymentPipelines"
$pipe  = $pipes.value | Where-Object { $_.displayName -eq $PipelineName }
if (-not $pipe) { throw "Deployment pipeline '$PipelineName' not found." }

# 2) Get its stages, then pick source/target by display name
$pipeDetails = GetJson "$base/deploymentPipelines/$($pipe.id)"
$src = $pipeDetails.stages | Where-Object { $_.displayName -eq $SourceStage }
$dst = $pipeDetails.stages | Where-Object { $_.displayName -eq $TargetStage }
if (-not $src) { throw "Source stage '$SourceStage' not found in pipeline '$PipelineName'." }
if (-not $dst) { throw "Target stage '$TargetStage' not found in pipeline '$PipelineName'." }

Write-Host "Pipeline: $($pipe.displayName) [$($pipe.id)]"
Write-Host "Source stage: $($src.displayName) [$($src.id)]"
Write-Host "Target stage: $($dst.displayName) [$($dst.id)]"

# 3) Build request body per official API
$body = @{
  sourceStageId = $src.id
  targetStageId = $dst.id
  note          = $Note
  options       = @{
    allowOverwriteArtifact = $true
    allowCreateArtifact    = $true
  }
}

# 4) Optional selective deploy (by displayName or by sourceItemId)
if ($ItemsJson -and $ItemsJson.Trim() -ne "") {
  $wanted = $ItemsJson | ConvertFrom-Json
  $itemsToSend = @()

  # Get the source stage items so we can map display names to IDs
  $stageItems = GetJson "$base/deploymentPipelines/$($pipe.id)/stages/$($src.id)/items"
  foreach ($w in $wanted) {
    if ($w.PSObject.Properties.Name -contains "sourceItemId" -and $w.sourceItemId -match '^[0-9a-fA-F-]{36}$') {
      $itemsToSend += @{ sourceItemId = $w.sourceItemId; itemType = $w.itemType }
      continue
    }

    if (-not ($w.PSObject.Properties.Name -contains "itemDisplayName")) {
      throw "Each item must have 'sourceItemId' or 'itemDisplayName'. Offending entry: $($w | ConvertTo-Json -Compress)"
    }

    $match = $stageItems.value |
      Where-Object { $_.itemDisplayName -eq $w.itemDisplayName -and $_.itemType -eq $w.itemType }

    if (-not $match) {
      throw "Item not found in source stage: '$($w.itemDisplayName)' [$($w.itemType)]."
    }

    # Use sourceItemId from the source stage
    $itemsToSend += @{ sourceItemId = $match.sourceItemId; itemType = $match.itemType }
  }

  $body.items = $itemsToSend
}

# 5) Kick off deployment (correct endpoint)
$deployUri = "$base/deploymentPipelines/$($pipe.id)/deploy"
$result = PostLro -Uri $deployUri -Body $body

Write-Host "✅ Deployment Succeeded."
if ($result) { $result | ConvertTo-Json -Depth 10 }
