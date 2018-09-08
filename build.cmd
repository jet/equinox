@echo off
cls

if not exist .paket\paket.exe (
    .paket\paket.bootstrapper.exe
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

if not ([%1]==[skipRestore=true]) (
    .paket\paket.exe restore
	dotnet restore
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

FAKE.exe run build.fsx %*