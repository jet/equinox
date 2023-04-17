#!/bin/bash

set -e # exit on any non-zero exit code

tmpf=$(mktemp)
curl -k https://localhost:8081/_explorer/emulator.pem > $tmpf
sudo security add-trusted-cert -d -r trustRoot -k ~/Library/Keychains/login.keychain $tmpf

dotnet run --project tools/Equinox.Tool -- init cosmos
dotnet run --project tools/Equinox.Tool -- init cosmos -c equinox-test-archive
