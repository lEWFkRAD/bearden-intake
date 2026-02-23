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

REM Check Tesseract is installed (check PATH first, then default locations)
tesseract --version >nul 2>&1
if %errorlevel% neq 0 (
    if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
        set "PATH=%PATH%;C:\Program Files\Tesseract-OCR"
        echo   Found Tesseract at C:\Program Files\Tesseract-OCR
    ) else if exist "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe" (
        set "PATH=%PATH%;C:\Program Files (x86)\Tesseract-OCR"
        echo   Found Tesseract at C:\Program Files ^(x86^)\Tesseract-OCR
    ) else (
        echo WARNING: Tesseract OCR not found.
        echo Download from https://github.com/UB-Mannheim/tesseract/wiki
        echo Install and restart this launcher.
        pause
        exit /b 1
    )
)

REM Check Poppler is installed (needed for PDF rendering)
where pdftoppm >nul 2>&1
if %errorlevel% neq 0 (
    if exist "C:\poppler\Library\bin\pdftoppm.exe" (
        set "PATH=%PATH%;C:\poppler\Library\bin"
        echo   Found Poppler at C:\poppler\Library\bin
    ) else if exist "C:\poppler\bin\pdftoppm.exe" (
        set "PATH=%PATH%;C:\poppler\bin"
        echo   Found Poppler at C:\poppler\bin
    ) else (
        echo WARNING: Poppler not found. PDF page rendering may fail.
        echo Download from https://github.com/oschwartz10612/poppler-windows/releases
    )
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
set PYTHONIOENCODING=utf-8
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
