param(
	[string] $verbosity="m",
	[switch][bool] $skipEs=$false,
	[string] $additionalMsBuildArgs
)

$args=@("/v:$verbosity","/fl","/bl","/p:skipes=$skipEs",$additionalMsBuildArgs)

Write-Host "dotnet msbuild $args"
. dotnet msbuild build.proj @args

if( $LASTEXITCODE -ne 0) {
	Write-Host "$message" -BackgroundColor DarkGreen "open msbuild.log for error info or rebuild with -v n/d/diag for more detail, or open msbuild.binlog using https://github.com/KirillOsenkov/MSBuildStructuredLog/releases/download/v2.0.40/MSBuildStructuredLogSetup.exe"
	exit $LASTEXITCODE
}