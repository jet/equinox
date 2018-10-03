@echo off
cls

if not ([%1]==[skipRestore=true]) (
    .paket\paket.exe restore
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

REM if not working, you forgot to `dotnet tool install fake-cli -g`
FAKE.exe run build.fsx %*