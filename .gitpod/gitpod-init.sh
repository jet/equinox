#! /usr/bin/env bash

# This script only updates the settings if it's run in a Gitpod workspace.
if [[ -n "${GITPOD_WORKSPACE_URL}" ]]; then
    # Workaround for: https://github.com/gitpod-io/gitpod/issues/5090
    cp -R /home/gitpod/dotnet /tmp/dotnet

    # Copy the VS Code settings used in Gitpod to make dev tooling work out of the box
    mkdir -p .vscode && cp .gitpod/vscode.settings.json .vscode/settings.json
fi

