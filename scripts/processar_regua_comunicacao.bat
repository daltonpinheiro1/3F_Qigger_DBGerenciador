@echo off
chcp 65001 >nul
title 3F Qigger - Régua de Comunicação WhatsApp
color 0E

echo ============================================
echo 3F Qigger - Regua de Comunicacao WhatsApp
echo Processamento da Base Analitica
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
echo Por favor, execute primeiro: PREPARACAO_COMPLETA.bat
echo Ou verifique: verificar_python.bat
echo.
pause
exit /b 1

:python_ok
echo [OK] Python encontrado usando: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

REM Mudar para o diretório raiz do projeto (pai de scripts/)
cd /d "%~dp0.."

REM Verificar se base_analitica_final.csv existe
set BASE_ANALITICA=G:\Meu Drive\3F Contact Center\base_analitica_final.csv
if not exist "%BASE_ANALITICA%" (
    echo [AVISO] Base analitica não encontrada: %BASE_ANALITICA%
    echo.
    echo Por favor, verifique se o arquivo existe no Google Drive.
    echo Você pode especificar outro caminho usando:
    echo   processar_regua_comunicacao.bat --base "caminho\para\arquivo.csv"
    echo.
    pause
    exit /b 1
)
echo [OK] Base analitica encontrada
echo.

echo ============================================
echo Iniciando processamento...
echo ============================================
echo.

%PYTHON_CMD% processar_regua_comunicacao.py %*
set RESULTADO=%ERRORLEVEL%

echo.
echo ============================================
if %RESULTADO% EQU 0 (
    echo Processamento concluído com sucesso!
    echo.
    echo Verifique o arquivo de saída:
    echo   G:\Meu Drive\3F Contact Center\WPP_Regua_Output.csv
) else (
    echo ERRO: Falha no processamento.
    echo.
    echo Verifique os erros acima e o log:
    echo   logs\regua_comunicacao.log
)
echo ============================================
echo.
pause
