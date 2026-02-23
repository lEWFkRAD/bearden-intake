@echo off
setlocal EnableDelayedExpansion
REM ─── Bearden Document Intake Platform — Windows Build Script ───
REM Run this on a Windows PC with Python installed.
REM It creates a distributable folder: dist\Bearden\
title Bearden Build

cd /d "%~dp0"

echo.
echo ====================================================
echo   Bearden Document Intake Platform — Build
echo ====================================================
echo.
echo   This will create a standalone distributable folder
echo   that runs without Python installed.
echo.
pause

REM ─── Check Python ───
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python is required to build. Install Python first.
    pause
    exit /b 1
)

REM ─── Install build dependencies ───
echo.
echo [1/5] Installing build dependencies...
pip install pyinstaller -q
pip install -r requirements.txt -q
echo   OK
echo.

REM ─── Check for Tesseract ───
echo [2/5] Locating Tesseract...
set "TESS_DIR="
if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
    set "TESS_DIR=C:\Program Files\Tesseract-OCR"
) else if exist "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe" (
    set "TESS_DIR=C:\Program Files (x86)\Tesseract-OCR"
) else (
    where tesseract >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('where tesseract') do set "TESS_DIR=%%~dpi"
    )
)
if "!TESS_DIR!"=="" (
    echo ERROR: Tesseract not found. Install Tesseract first, then re-run build.bat.
    pause
    exit /b 1
)
echo   Found: !TESS_DIR!
echo.

REM ─── Check for Poppler ───
echo [3/5] Locating Poppler...
set "POPPLER_DIR="
if exist "C:\poppler\Library\bin\pdftoppm.exe" (
    set "POPPLER_DIR=C:\poppler\Library\bin"
) else if exist "C:\poppler\bin\pdftoppm.exe" (
    set "POPPLER_DIR=C:\poppler\bin"
) else (
    where pdftoppm >nul 2>&1
    if %errorlevel% equ 0 (
        for /f "tokens=*" %%i in ('where pdftoppm') do set "POPPLER_DIR=%%~dpi"
    )
)
if "!POPPLER_DIR!"=="" (
    echo ERROR: Poppler not found. Install Poppler first, then re-run build.bat.
    pause
    exit /b 1
)
echo   Found: !POPPLER_DIR!
echo.

REM ─── Run PyInstaller ───
echo [4/5] Building executable (this takes a few minutes)...
pyinstaller ^
    --name Bearden ^
    --noconfirm ^
    --clean ^
    --add-data "extract.py;." ^
    --add-data "requirements.txt;." ^
    --hidden-import anthropic ^
    --hidden-import flask ^
    --hidden-import openpyxl ^
    --hidden-import pdf2image ^
    --hidden-import pytesseract ^
    --hidden-import PIL ^
    --hidden-import fitz ^
    --hidden-import pymupdf ^
    --collect-all anthropic ^
    --collect-all flask ^
    --collect-all openpyxl ^
    --collect-submodules pymupdf ^
    app.py

if %errorlevel% neq 0 (
    echo.
    echo BUILD FAILED. Check the errors above.
    pause
    exit /b 1
)
echo   OK
echo.

REM ─── Bundle Tesseract and Poppler ───
echo [5/5] Bundling Tesseract and Poppler...

REM Copy Tesseract
xcopy "!TESS_DIR!" "dist\Bearden\tesseract\" /E /I /Q /Y >nul
echo   Tesseract copied.

REM Copy Poppler
xcopy "!POPPLER_DIR!" "dist\Bearden\poppler\" /E /I /Q /Y >nul
echo   Poppler copied.

REM Copy extract.py alongside the exe (subprocess needs it as a file)
copy /Y "extract.py" "dist\Bearden\" >nul

REM Create data folders
mkdir "dist\Bearden\data" 2>nul
mkdir "dist\Bearden\data\uploads" 2>nul
mkdir "dist\Bearden\data\outputs" 2>nul
mkdir "dist\Bearden\data\page_images" 2>nul
mkdir "dist\Bearden\clients" 2>nul
mkdir "dist\Bearden\verifications" 2>nul

REM Create config file for API key
(
echo # Bearden Document Intake Platform — Configuration
echo # Paste your Anthropic API key below ^(replace the placeholder^):
echo ANTHROPIC_API_KEY=sk-ant-paste-your-key-here
) > "dist\Bearden\config.txt"

REM Create the launcher that reads config and starts the app
(
echo @echo off
echo setlocal EnableDelayedExpansion
echo title Bearden Document Intake Platform
echo cd /d "%%~dp0"
echo.
echo REM Read API key from config.txt
echo for /f "tokens=1,* delims==" %%%%a in ^('findstr /B "ANTHROPIC_API_KEY" config.txt'^) do set "ANTHROPIC_API_KEY=%%%%b"
echo.
echo REM Set paths to bundled Tesseract and Poppler
echo set "PATH=%%~dp0tesseract;%%~dp0poppler;%%PATH%%"
echo set "TESSDATA_PREFIX=%%~dp0tesseract\tessdata"
echo.
echo if "%%ANTHROPIC_API_KEY%%"=="sk-ant-paste-your-key-here" ^(
echo     echo.
echo     echo   ERROR: Please edit config.txt and paste your API key.
echo     echo   Then run this launcher again.
echo     echo.
echo     pause
echo     exit /b 1
echo ^)
echo.
echo set PORT=5050
echo echo.
echo echo ====================================================
echo echo   Bearden Document Intake Platform
echo echo   Starting on http://localhost:5050
echo echo ====================================================
echo echo.
echo echo   Close this window to stop the server.
echo echo.
echo start "" cmd /c "timeout /t 3 /nobreak ^>nul ^&^& start http://localhost:5050"
echo Bearden.exe
) > "dist\Bearden\Start Bearden.bat"

echo.
echo ====================================================
echo   Build complete!
echo.
echo   Distributable folder: dist\Bearden\
echo.
echo   To deploy:
echo     1. Copy the dist\Bearden folder to any Windows PC
echo     2. Edit config.txt with the API key
echo     3. Double-click "Start Bearden.bat"
echo.
echo   No Python or other installs needed!
echo ====================================================
echo.
pause
