param(
	[string] $verbosity="m",
	[Alias("s")][switch][bool] $skipStores=$false,
	[Alias("se")][switch][bool] $skipEs=$skipStores,
	[Alias("sc")][switch][bool] $skipCosmos=$skipStores,
	[Alias("scp")][switch][bool] $skipProvisionCosmos=$false,
	[Alias("scd")][switch][bool] $skipDeprovisionCosmos=$false,
	[string] $additionalMsBuildArgs="-t:Build"
)

$args=@("/v:$verbosity","/fl","/bl",$additionalMsBuildArgs)

function warn ($msg) { Write-Host "$msg" -BackgroundColor DarkGreen }

# Yes, this leaves the value set on exit, but I want to keep the script legible
$env:EQUINOX_INTEGRATION_SKIP_EVENTSTORE=[string]$skipEs
if ($skipEs) { warn "Skipping EventStore tests" }

if ($skipCosmos) {
    warn "Skipping Cosmos tests" as requested
} elseif ($skipProvisionCosmos -or -not $env:EQUINOX_COSMOS_CONNECTION -or -not $env:EQUINOX_COSMOS_COLLECTION) {
    warn "Skipping Provisioning Cosmos"
} else {
    warn "Provisioning cosmos..."
    $collection=[guid]::NewGuid()
    cli/Equinox.Cli/bin/Release/net461/Equinox.Cli cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 10000
}
$env:EQUINOX_INTEGRATION_SKIP_COSMOS=[string]$skipCosmos

Write-Host "dotnet msbuild $args"
. dotnet msbuild build.proj @args

if( $LASTEXITCODE -ne 0) {
	warn "open msbuild.log for error info or rebuild with -v n/d/diag for more detail, or open msbuild.binlog using https://github.com/KirillOsenkov/MSBuildStructuredLog/releases/download/v2.0.40/MSBuildStructuredLogSetup.exe"
	exit $LASTEXITCODE
}

if (-not $skipCosmos -and -not $skipDeprovisionCosmos) {
    warn "Deprovisioning Cosmos"
    cli/Equinox.Cli/bin/Release/net461/Equinox.Cli cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 0
}