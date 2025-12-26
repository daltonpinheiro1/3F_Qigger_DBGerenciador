@echo off
chcp 65001 >nul
title 3F Qigger - Régua de Comunicação DINÂMICA
color 0E

echo ============================================
echo 3F Qigger - Regua de Comunicacao DINAMICA
echo Cruzamento de multiplas fontes de dados
echo ============================================
echo.
echo Fontes de dados:
echo   1. Base Analitica (vendas/clientes)
echo   2. Relatorio de Objetos (logistica)
echo   3. CSV Siebel (portabilidade)
echo.
echo Chave de cruzamento: Proposta iSize
echo ============================================
echo.

REM Verificar Python
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

echo [ERRO] Python não encontrado!
pause
exit /b 1

:python_ok
echo [OK] Python encontrado: %PYTHON_CMD%
echo.

REM Mudar para o diretório raiz do projeto (pai de scripts/)
cd /d "%~dp0.."

echo Iniciando processamento dinamico...
echo.

%PYTHON_CMD% processar_regua_dinamica.py
set RESULTADO=%ERRORLEVEL%

echo.
echo ============================================
if %RESULTADO% EQU 0 (
    echo Processamento concluído com sucesso!
    echo.
    echo Arquivo de saída:
    echo   G:\Meu Drive\3F Contact Center\WPP_Regua_Dinamica.csv
) else (
    echo ERRO: Falha no processamento.
    echo Verifique: logs\regua_dinamica.log
)
echo ============================================
echo.
pause
