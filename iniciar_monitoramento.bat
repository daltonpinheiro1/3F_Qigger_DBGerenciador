@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Monitoramento
color 0A

echo ============================================
echo 3F Qigger DB Gerenciador
echo Monitoramento Automático
echo ============================================
echo.

REM Verificar Python - tentar diferentes comandos
set PYTHON_CMD=
python --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python
    goto :python_ok
)

py --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=py
    goto :python_ok
)

python3 --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python3
    goto :python_ok
)

echo ERRO: Python não encontrado!
echo Execute primeiro: PREPARACAO_COMPLETA.bat
pause
exit /b 1

:python_ok
REM Mudar para o diretório do script
cd /d "%~dp0"

REM Executar o monitoramento
echo Iniciando monitoramento...
echo Pressione Ctrl+C para parar.
echo.

%PYTHON_CMD% iniciar_monitoramento.py

if errorlevel 1 (
    echo.
    echo ERRO: Falha ao iniciar o monitoramento.
    pause
    exit /b 1
)

pause

