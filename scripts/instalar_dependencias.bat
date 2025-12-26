@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Instalar Dependências
color 0E

echo ============================================
echo 3F Qigger DB Gerenciador
echo Instalação de Dependências
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
echo.
echo Por favor, instale o Python 3.8 ou superior.
echo Download: https://www.python.org/downloads/
echo.
echo Durante a instalação, marque: "Add Python to PATH"
echo.
pause
exit /b 1

:python_ok
echo Python encontrado usando: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

REM Mudar para o diretório do script
cd /d "%~dp0"

echo Atualizando pip...
%PYTHON_CMD% -m pip install --upgrade pip
echo.

echo Instalando dependências do requirements.txt...
%PYTHON_CMD% -m pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo ERRO: Falha ao instalar algumas dependências.
    echo Verifique os erros acima.
    pause
    exit /b 1
)

echo.
echo ============================================
echo Instalação concluída com sucesso!
echo ============================================
echo.
echo Dependências instaladas:
echo   - python-dateutil (manipulação de datas)
echo   - watchdog (monitoramento de pastas)
echo.
pause

