param(
  [Parameter(Mandatory=$true)][string]$TenantId,
  [Parameter(Mandatory=$true)][string]$ClientId,
  [Parameter(Mandatory=$true)][string]$ClientSecret,
  [Parameter(Mandatory=$true)][string]$PipelineName,
  [string]$SourceStageName = "Development",
  [string]$TargetStageName = "Production",
  [string]$Note = "",
  [string]$ItemsJson = ""
)

function Get-FabricToken {
  param($TenantId,$ClientId,$ClientSecret)
  $body = @{
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://api.fabric.microsoft.com/.default"
    grant_type    = "client_credentials"
  }
  (Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" `
    -Body $body -ContentType "application/x-www-form-urlencoded").access_token
}

function GetJson($uri,$tok){ Invoke-RestMethod -Headers @{Authorization="Bearer $tok"} -Uri $uri }
function PostLro($uri,$tok,$obj){
  $resp = Invoke-WebRequest -Method POST -Uri $uri -Headers @{Authorization="Bearer $tok"} `
          -ContentType "application/json" -Body ($obj|ConvertTo-Json -Depth 20)
  @{
    Location          = $resp.Headers["Location"]
    OperationLocation = ($resp.Headers["Operation-Location"] ?? $resp.Headers["operation-location"])
  }
}
function WaitOp($tok,$opUri){
  do{ Start-Sleep 5; $st = GetJson $opUri $tok; Write-Host "Status: $($st.status)" }
  while ($st.status -in @("NotStarted","Running"))
  if($st.status -ne "Succeeded"){ throw "Deploy failed. Details:`n$($st|ConvertTo-Json -Depth 20)" }
}

$tok = Get-FabricToken $TenantId $ClientId $ClientSecret

$pipe = (GetJson "https://api.fabric.microsoft.com/v1/deploymentPipelines" $tok).value `
        | Where-Object { $_.displayName -eq $PipelineName }
if(-not $pipe){ throw "Deployment pipeline '$PipelineName' not found." }

$stages = (GetJson "https://api.fabric.microsoft.com/v1/deploymentPipelines/$($pipe.id)/stages" $tok).value
$src = $stages | ? { $_.displayName -eq $SourceStageName } ; if(-not $src){ throw "Stage '$SourceStageName' not found." }
$dst = $stages | ? { $_.displayName -eq $TargetStageName } ; if(-not $dst){ throw "Stage '$TargetStageName' not found." }

$body = @{ note = @{ content = $Note } }
if($ItemsJson -and $ItemsJson.Trim()){
  $items = $ItemsJson | ConvertFrom-Json
  $body.itemsToDeploy = @()
  foreach($i in $items){ $body.itemsToDeploy += @{ itemDisplayName=$i.itemDisplayName; itemType=$i.itemType } }
}

$uri = "https://api.fabric.microsoft.com/v1/deploymentPipelines/$($pipe.id)/stages/$($src.id)/deploy?targetStageId=$($dst.id)&allowOverwrite=true"
$lro = PostLro $uri $tok $body
$op = $lro.OperationLocation ; if(-not $op){ $op = $lro.Location }
if(-not $op){ throw "No operation location returned." }
Write-Host "Submitted deploy. Waiting on $op ..."
WaitOp $tok $op
Write-Host "Deployment Succeeded."
