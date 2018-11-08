param(
	[string] $verbosity="m",
	[Alias("s")][switch][bool] $skipStores=$false,
	[Alias("se")][switch][bool] $skipEs=$skipStores,
	[string] $additionalMsBuildArgs="-t:Build"
)

$args=@("/v:$verbosity","/fl","/bl",$additionalMsBuildArgs)

function warn ($msg) { Write-Host "$msg" -BackgroundColor DarkGreen }

# Yes, this leaves the value set on exit, but I want to keep the script legible
$env:EQUINOX_INTEGRATION_SKIP_EVENTSTORE=[string]$skipEs
if ($skipEs) { warn "Skipping EventStore tests" }

warn "RUNNING: dotnet msbuild $args"
. dotnet msbuild build.proj @args

if( $LASTEXITCODE -ne 0) {
	warn "open msbuild.log for error info or rebuild with -v n/d/diag for more detail, or open msbuild.binlog using https://github.com/KirillOsenkov/MSBuildStructuredLog/releases/download/v2.0.40/MSBuildStructuredLogSetup.exe"
	exit $LASTEXITCODE
}
