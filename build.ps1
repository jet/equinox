param(
	[string] $verbosity="m",
	[Alias("sc")][switch][bool] $skipCosmos=$false,
	[Alias("se")][switch][bool] $skipEs=$false,
	[string] $additionalMsBuildArgs
)

$args=@("/v:$verbosity","/fl","/bl",$additionalMsBuildArgs)

function warn ($msg) { Write-Host "$msg" -BackgroundColor DarkGreen }

# Yes, this leaves the value set on exit, but I want to keep the script legible
if ($skipEs) { warn "Skipping EventStore tests" }
$env:EQUINOX_INTEGRATION_SKIP_EVENTSTORE=[string]$skipEs
if ($skipCosmos) { warn "Skipping Cosmos tests" }
$env:EQUINOX_INTEGRATION_SKIP_COSMOS=[string]$skipCosmos

Write-Host "dotnet msbuild $args"
. dotnet msbuild build.proj @args

if( $LASTEXITCODE -ne 0) {
	warn "open msbuild.log for error info or rebuild with -v n/d/diag for more detail, or open msbuild.binlog using https://github.com/KirillOsenkov/MSBuildStructuredLog/releases/download/v2.0.40/MSBuildStructuredLogSetup.exe"
	exit $LASTEXITCODE
}