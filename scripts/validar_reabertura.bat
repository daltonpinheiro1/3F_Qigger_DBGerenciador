@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Validar Reabertura
color 0E

echo ============================================
echo 3F Qigger DB Gerenciador v2.0
echo Validador de Reabertura
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

echo [ERRO] Python não encontrado!
echo.
pause
exit /b 1

:python_ok
echo [OK] Python encontrado usando: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

REM Mudar para o diretório raiz do projeto
cd /d "%~dp0.."

REM Executar validação
%PYTHON_CMD% validar_reabertura.py
set RESULTADO=%ERRORLEVEL%

echo.
echo ============================================
if %RESULTADO% EQU 0 (
    echo Validação concluída com sucesso!
) else (
    echo ERRO: Falha na validação.
)
echo ============================================
echo.
pause

