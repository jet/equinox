#!/bin/bash

set -e # exit on any non-zero exit code

if [ "$EQUINOX_COSMOS_CONNECTION" == "TrustLocalEmulator=true" ]; then 
    echo "Skipping downloading/trusting CosmosDb Emulator Certificate as \$EQUINOX_COSMOS_CONNECTION == \"TrustLocalEmulator=true\""
else
    echo "Downloading/trusting CosmosDb Emulator Certificate as \$EQUINOX_COSMOS_CONNECTION is not \"TrustLocalEmulator=true\""
    tmpf=$(mktemp)
    curl -k https://localhost:8081/_explorer/emulator.pem > $tmpf
    sudo security add-trusted-cert -d -r trustRoot -k ~/Library/Keychains/login.keychain $tmpf
fi

dotnet run -c Release --project tools/Equinox.Tool -- init cosmos
dotnet run -c Release --project tools/Equinox.Tool -- init cosmos -c equinox-test-archive

# Explorer URL: https://localhost:8081/_explorer/index.html, see https://learn.microsoft.com/en-us/azure/cosmos-db/how-to-develop-emulator