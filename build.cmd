@echo off
cls

if not ([%1]==[skipRestore=true]) (
    .paket\paket.exe restore
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

FAKE.exe run build.fsx %*