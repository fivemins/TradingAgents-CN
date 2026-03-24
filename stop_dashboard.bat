@echo off
setlocal
set ROOT=%~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\stop_dashboard.ps1"
if errorlevel 1 (
  echo.
  echo Dashboard stop command failed.
  pause
)
