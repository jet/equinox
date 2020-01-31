param(
	[string] $verbosity="m",
	[Alias("s")][switch][bool] $skipStores=$false,
	[Alias("se")][switch][bool] $skipEs=$skipStores,
	[Alias("sc")][switch][bool] $skipCosmos=$skipStores,
	[Alias("cs")][string] $cosmosServer=$env:EQUINOX_COSMOS_CONNECTION,
	[Alias("cd")][string] $cosmosDatabase=$env:EQUINOX_COSMOS_DATABASE,
	[Alias("cc")][string] $cosmosContainer=$env:EQUINOX_COSMOS_CONTAINER,
	[Alias("scp")][switch][bool] $skipProvisionCosmos=$skipCosmos -or -not $cosmosServer -or -not $cosmosDatabase -or -not $cosmosContainer,
	[Alias("scd")][switch][bool] $skipDeprovisionCosmos=$skipProvisionCosmos,
	[string] $additionalMsBuildArgs="-t:Build"
)

$args=@("/v:$verbosity","/fl","/bl",$additionalMsBuildArgs)

function warn ($msg) { Write-Host "$msg" -BackgroundColor DarkGreen }

# Yes, this leaves the value set on exit, but I want to keep the script legible
$env:EQUINOX_INTEGRATION_SKIP_EVENTSTORE=[string]$skipEs
if ($skipEs) { warn "Skipping EventStore tests" }

function cliCosmos($arghs) {
	Write-Host "dotnet run tools/Equinox.Tool -- $arghs cosmos -s <REDACTED> -d $cosmosDatabase -c $cosmosContainer"
	dotnet run -p tools/Equinox.Tool -f netcoreapp3.1 -- @arghs cosmos -s $cosmosServer -d $cosmosDatabase -c $cosmosContainer
}

if ($skipCosmos) {
	warn "Skipping Cosmos tests as requested"
} elseif ($skipProvisionCosmos) {
	warn "Skipping Provisioning Cosmos"
} else {
	warn "Provisioning cosmos (without stored procedure)..."
	# -P: inhibit creation of stored proc (everything in the repo should work without it due to auto-provisioning)
	cliCosmos @("init", "-ru", "400", "-P")
	$deprovisionCosmos=$true
}
$env:EQUINOX_INTEGRATION_SKIP_COSMOS=[string]$skipCosmos

warn "RUNNING: dotnet msbuild $args"
. dotnet msbuild build.proj @args

if( $LASTEXITCODE -ne 0) {
	warn "open msbuild.log for error info or rebuild with -v n/d/diag for more detail, or open msbuild.binlog using https://github.com/KirillOsenkov/MSBuildStructuredLog/releases/download/v2.0.40/MSBuildStructuredLogSetup.exe"
	exit $LASTEXITCODE
}

if (-not $skipDeprovisionCosmos) {
	warn "Deprovisioning Cosmos"
	throw "Deprovisioning step not implemented yet - please deallocate your resources using the Azure Portal"
}
