#!/bin/bash

docker exec -it equinox-mssql /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P "mssql1Ipw" \
    -Q "CREATE database EQUINOX_TEST_DB"
