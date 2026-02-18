@echo off
REM ─── Bearden Document Intake Platform — Windows Launcher ───
title Bearden Document Intake Platform

cd /d "%~dp0"

REM Check Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is not installed or not in PATH.
    echo Download from https://python.org and check "Add Python to PATH" during install.
    pause
    exit /b 1
)

REM Check Tesseract is installed
tesseract --version >nul 2>&1
if %errorlevel% neq 0 (
    echo WARNING: Tesseract OCR not found in PATH.
    echo Download from https://github.com/UB-Mannheim/tesseract/wiki
    echo Install and add to PATH, then restart this launcher.
    pause
    exit /b 1
)

REM Check API key is set
if "%ANTHROPIC_API_KEY%"=="" (
    echo ERROR: ANTHROPIC_API_KEY is not set.
    echo Run this in Command Prompt:
    echo   setx ANTHROPIC_API_KEY "sk-ant-your-key-here"
    echo Then close and reopen this launcher.
    pause
    exit /b 1
)

set PORT=5050
echo.
echo ====================================================
echo   Bearden Document Intake Platform
echo   Starting on http://localhost:5050
echo ====================================================
echo.
echo   Close this window to stop the server.
echo.

REM Open browser after short delay
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:5050"

REM Run the server (keeps this window open)
python app.py
