param(
    [alias("sc")][Switch][bool]$skipCert = $false,
    [alias("l")][Switch][bool]$bootstrapEqx = $false,
    [alias("si")][Switch][bool]$skipInit = $false
)

if (-not $skipCert) {
    Write-Host "Please click on the cert import popup to confirm..."
    $rgs = @{
        Uri = 'https://localhost:8081/_explorer/emulator.pem'
        Method = 'GET'
        OutFile = "$env:TEMP/emulatorcert.crt"
        SkipCertificateCheck = $True
    }
    Invoke-WebRequest @rgs
    $rgs = @{
        FilePath = "$env:TEMP/emulatorcert.crt"
        CertStoreLocation = 'Cert:\CurrentUser\Root'
    }
    Import-Certificate @rgs
}

$env:EQUINOX_COSMOS_CONNECTION = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;"
$env:EQUINOX_COSMOS_DATABASE = "equinox-test"
$env:EQUINOX_COSMOS_CONTAINER = "equinox-test"

if (-not $skipInit)
{
    $cmd = $bootstrapEqx ? "dotnet run -c Release --project tools/Equinox.Tool --" : "eqx"
    Invoke-Expression "$cmd init cosmos"
    Invoke-Expression "$cmd init cosmos -c equinox-test-archive"
}
# Explorer URL: https://localhost:8080/_explorer/index.html, see https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-develop-emulator