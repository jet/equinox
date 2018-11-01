param(
	[string] $verbosity="m",
	[Alias("sc")][switch][bool] $skipCosmos=$false,
	[Alias("scp")][switch][bool] $skipProvisionCosmos=$false,
	[Alias("scd")][switch][bool] $skipDeprovisionCosmos=$false,
	[Alias("se")][switch][bool] $skipEs=$false,
	[string] $additionalMsBuildArgs
)

$args=@("/v:$verbosity","/fl","/bl",$additionalMsBuildArgs)

function warn ($msg) { Write-Host "$msg" -BackgroundColor DarkGreen }

# Yes, this leaves the value set on exit, but I want to keep the script legible
if ($skipEs) { warn "Skipping EventStore tests" }

$env:EQUINOX_INTEGRATION_SKIP_EVENTSTORE=[string]$skipEs
if ($skipCosmos) {
    warn "Skipping Cosmos tests" as requested
} elseif ($skipProvisionCosmos -or -not $env:EQUINOX_COSMOS_CONNECTION -or -not $env:EQUINOX_COSMOS_COLLECTION) {
    warn "Skipping Provisioning Cosmos"
} else {
    warn "Provisioning cosmos..."
    $collection=[guid]::NewGuid()
    benchmarks/Equinox.Bench/bin/Release/net461/Equinox.Bench cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 10000
	$deprovisionCosmos=$true
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
    benchmarks/Equinox.Bench/bin/Release/net461/Equinox.Bench cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 0
}
