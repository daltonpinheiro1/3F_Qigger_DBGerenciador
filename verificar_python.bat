@echo off
chcp 65001 >nul
title Verificar Instalação do Python
color 0E

echo ============================================
echo Verificação de Instalação do Python
echo ============================================
echo.

echo Tentando encontrar Python...
echo.

REM Tentar python
echo [1/3] Tentando: python
python --version >nul 2>&1
if not errorlevel 1 (
    echo    ✓ SUCESSO!
    python --version
    echo.
    echo Comando a usar: python
    echo.
    pause
    exit /b 0
)
echo    ✗ Não encontrado

REM Tentar py
echo [2/3] Tentando: py
py --version >nul 2>&1
if not errorlevel 1 (
    echo    ✓ SUCESSO!
    py --version
    echo.
    echo Comando a usar: py
    echo.
    pause
    exit /b 0
)
echo    ✗ Não encontrado

REM Tentar python3
echo [3/3] Tentando: python3
python3 --version >nul 2>&1
if not errorlevel 1 (
    echo    ✓ SUCESSO!
    python3 --version
    echo.
    echo Comando a usar: python3
    echo.
    pause
    exit /b 0
)
echo    ✗ Não encontrado

echo.
echo ============================================
echo Python NÃO encontrado!
echo ============================================
echo.
echo SOLUÇÕES:
echo.
echo 1. INSTALAR PYTHON:
echo    - Acesse: https://www.python.org/downloads/
echo    - Baixe Python 3.8 ou superior
echo    - Durante a instalação, MARQUE:
echo      ☑ "Add Python to PATH"
echo    - Após instalar, FECHE E REABRA este terminal
echo.
echo 2. SE JÁ TEM PYTHON INSTALADO:
echo    - Reinicie o computador
echo    - Ou adicione manualmente ao PATH do sistema
echo.
pause
exit /b 1

