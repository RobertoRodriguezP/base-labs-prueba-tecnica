@echo off
setlocal

echo -------------------------------
echo Annie’s Magic Numbers Setup 🧙
echo -------------------------------

:: 1. Check if Python is installed
where python >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo Python not found. Downloading Python 3.11...
    curl -o python-installer.exe https://www.python.org/ftp/python/3.11.8/python-3.11.8-amd64.exe

    echo Installing Python...
    python-installer.exe /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
    if %ERRORLEVEL% NEQ 0 (
        echo Python installation failed. Aborting.
        exit /b 1
    )
    echo Python installed successfully.
    del python-installer.exe
) else (
    echo Python is already installed.
)

:: 2. Verify pip
python -m ensurepip
python -m pip install --upgrade pip

:: 3. Create virtual environment
echo Creating virtual environment...
python -m venv env
if exist env\Scripts\activate.bat (
    call env\Scripts\activate.bat
) else (
    echo Failed to create virtual environment.
    exit /b 1
)

:: 4. Install requirements
echo Installing required Python packages...
pip install -r requirements.txt

if %ERRORLEVEL% NEQ 0 (
    echo  Failed to install some packages. Check requirements.txt
    exit /b 1
)

echo  All done! Environment is ready.
echo To activate it later, run: env\Scripts\activate

endlocal
pause
