@echo off
REM =============================================
REM Company Enrichment Scraper - BASE - Windows Launcher
REM =============================================
REM Enriches: Company Name, Revenue Size, Revenue Evidence,
REM           Industry, Employee Size, Region, Company Website
REM
REM Usage (all flag-style, passed through to Python):
REM   run_company_enrichment.bat --input "C:\...\chunk.csv" --server-id 1 --server-count 5 --log-prefix job_123
REM   run_company_enrichment.bat --server-id 1 --limit 50
REM   run_company_enrichment.bat --fresh
REM =============================================

REM Run from the directory where this batch file lives
cd /d "%~dp0"

echo.
echo =============================================
echo Company Enrichment Scraper - BASE
echo =============================================
echo.

REM Prefer a dedicated Python next to the script
set PYTHON_EXE=python
if exist "%~dp0python-3.10.2.amd64\python.exe" set PYTHON_EXE=%~dp0python-3.10.2.amd64\python.exe
if exist "%~dp0..\python-3.10.2.amd64\python.exe" set PYTHON_EXE=%~dp0..\python-3.10.2.amd64\python.exe

echo Verifying directories...
if not exist "queue"       mkdir "queue"
if not exist "processing"  mkdir "processing"
if not exist "processed"   mkdir "processed"
if not exist "logs"        mkdir "logs"
echo Directories verified.

echo.
echo Starting scraper...
"%PYTHON_EXE%" "%~dp0company_enrichment.py" %*

echo.
echo =============================================
echo Scraper finished or stopped.
echo Check 'processed' for CSV results, 'logs' for run logs.
echo =============================================
pause
