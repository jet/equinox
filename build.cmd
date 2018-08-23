@echo off
cls

if not ([%1]==[skipRestore=true]) (
    .paket\paket.exe restore
    if errorlevel 1 (
        exit /b %errorlevel%
    )
)

packages\FAKE\tools\FAKE.exe build.fsx %*
