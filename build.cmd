@echo off
cls

if not exist .paket\paket.exe (
    .paket\paket.bootstrapper.exe
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

if not exist packages (
    .paket\paket.exe restore
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

packages\build\FAKE\tools\FAKE.exe build.fsx %*