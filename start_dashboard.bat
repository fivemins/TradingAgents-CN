@echo off
setlocal
set ROOT=%~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\start_dashboard.ps1"
if errorlevel 1 (
  echo.
  echo Dashboard launcher failed.
  pause
)
