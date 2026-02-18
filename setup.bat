@echo off
setlocal EnableDelayedExpansion
REM ─── Bearden Document Intake Platform — Windows Setup ───
title Bearden Setup
cd /d "%~dp0"

echo.
echo ====================================================
echo   Bearden Document Intake Platform — Setup
echo ====================================================
echo.
echo   This will install everything needed to run the
echo   platform on this PC. It only needs to run once.
echo.
pause

REM ─── Step 1: Check Python ───
echo.
echo [1/5] Checking Python...
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo  Python is not installed.
    echo  Opening the download page now...
    echo.
    echo  IMPORTANT: During install, CHECK the box that says
    echo  "Add Python to PATH" at the bottom of the installer!
    echo.
    start "" "https://www.python.org/downloads/"
    echo  After installing Python, CLOSE this window and run setup.bat again.
    pause
    exit /b 1
)
for /f "tokens=*" %%i in ('python --version 2^>^&1') do echo   Found: %%i
echo   OK
echo.

REM ─── Step 2: Install Python packages ───
echo [2/5] Installing Python packages...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo  ERROR: pip install failed. Make sure Python is installed correctly.
    pause
    exit /b 1
)
echo   OK
echo.

REM ─── Step 3: Install Tesseract OCR ───
echo [3/5] Checking Tesseract OCR...
tesseract --version >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo   Tesseract not found. Downloading installer...
    echo.

    REM Download Tesseract installer
    powershell -Command "& { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://digi.bib.uni-mannheim.de/tesseract/tesseract-ocr-w64-setup-5.5.0.20241111.exe' -OutFile '%TEMP%\tesseract-setup.exe' }"
    if not exist "%TEMP%\tesseract-setup.exe" (
        echo   Download failed. Opening manual download page...
        start "" "https://github.com/UB-Mannheim/tesseract/wiki"
        echo   Install Tesseract, then run setup.bat again.
        pause
        exit /b 1
    )

    echo   Running Tesseract installer...
    echo   Accept the defaults and click through the installer.
    echo.
    start /wait "" "%TEMP%\tesseract-setup.exe"
    del "%TEMP%\tesseract-setup.exe" 2>nul

    REM Add default Tesseract path to this session
    set "PATH=%PATH%;C:\Program Files\Tesseract-OCR"

    REM Add to user PATH permanently
    powershell -Command "& { $current = [Environment]::GetEnvironmentVariable('Path', 'User'); if ($current -notlike '*Tesseract-OCR*') { [Environment]::SetEnvironmentVariable('Path', $current + ';C:\Program Files\Tesseract-OCR', 'User') } }"

    tesseract --version >nul 2>&1
    if %errorlevel% neq 0 (
        echo.
        echo   Tesseract installed but not found in PATH.
        echo   Close this window and run setup.bat again.
        pause
        exit /b 1
    )
)
echo   OK
echo.

REM ─── Step 4: Install Poppler ───
echo [4/5] Checking Poppler (PDF renderer)...
where pdftoppm >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo   Poppler not found. Downloading...
    echo.

    REM Download Poppler
    powershell -Command "& { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://github.com/oschwartz10612/poppler-windows/releases/download/v24.08.0-0/Release-24.08.0-0.zip' -OutFile '%TEMP%\poppler.zip' }"
    if not exist "%TEMP%\poppler.zip" (
        echo   Download failed. Opening manual download page...
        start "" "https://github.com/oschwartz10612/poppler-windows/releases"
        echo   Extract poppler to C:\poppler and add C:\poppler\Library\bin to PATH.
        echo   Then run setup.bat again.
        pause
        exit /b 1
    )

    echo   Extracting to C:\poppler...
    powershell -Command "& { Expand-Archive -Path '%TEMP%\poppler.zip' -DestinationPath 'C:\poppler-temp' -Force }"
    del "%TEMP%\poppler.zip" 2>nul

    REM The zip extracts to a subfolder — move contents up
    if exist "C:\poppler" rmdir /s /q "C:\poppler"
    for /d %%D in (C:\poppler-temp\poppler-*) do move "%%D" "C:\poppler" >nul 2>&1
    rmdir /s /q "C:\poppler-temp" 2>nul

    REM Add to this session
    set "PATH=%PATH%;C:\poppler\Library\bin"

    REM Add to user PATH permanently
    powershell -Command "& { $current = [Environment]::GetEnvironmentVariable('Path', 'User'); if ($current -notlike '*poppler*') { [Environment]::SetEnvironmentVariable('Path', $current + ';C:\poppler\Library\bin', 'User') } }"

    where pdftoppm >nul 2>&1
    if %errorlevel% neq 0 (
        echo   Poppler installed but not found. Trying alternate path...
        if exist "C:\poppler\bin\pdftoppm.exe" (
            set "PATH=%PATH%;C:\poppler\bin"
            powershell -Command "& { $current = [Environment]::GetEnvironmentVariable('Path', 'User'); if ($current -notlike '*poppler\bin*') { [Environment]::SetEnvironmentVariable('Path', $current + ';C:\poppler\bin', 'User') } }"
            echo   Found at C:\poppler\bin
        ) else (
            echo   Could not locate poppler binaries.
            echo   Close this window and run setup.bat again.
            pause
            exit /b 1
        )
    )
)
echo   OK
echo.

REM ─── Step 5: API Key ───
echo [5/5] Checking Anthropic API key...
if "%ANTHROPIC_API_KEY%"=="" (
    echo.
    echo   No API key found.
    echo.
    set /p "API_KEY=  Paste your Anthropic API key here: "
    if not "!API_KEY!"=="" (
        setx ANTHROPIC_API_KEY "!API_KEY!" >nul 2>&1
        set "ANTHROPIC_API_KEY=!API_KEY!"
        echo   API key saved.
    ) else (
        echo   No key entered. You can set it later with:
        echo     setx ANTHROPIC_API_KEY "sk-ant-your-key-here"
    )
) else (
    echo   API key is set.
)
echo   OK
echo.

REM ─── Create data folders ───
if not exist "data" mkdir data
if not exist "data\uploads" mkdir data\uploads
if not exist "data\outputs" mkdir data\outputs
if not exist "data\page_images" mkdir data\page_images
if not exist "clients" mkdir clients
if not exist "verifications" mkdir verifications

echo.
echo ====================================================
echo   Setup complete!
echo.
echo   Double-click launch.bat to start the platform.
echo ====================================================
echo.
pause
