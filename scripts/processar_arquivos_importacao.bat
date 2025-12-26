@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Processar Arquivos de Importação
color 0A

echo ============================================
echo 3F Qigger DB Gerenciador v3.0
echo Processar Arquivos de Importação
echo Com suporte a:
echo   - triggers.xlsx (regras dinamicas)
echo   - Relatorio de Objetos (logistica)
echo   - Regua de Comunicacao WPP
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

REM Verificar se triggers.xlsx existe
if not exist "triggers.xlsx" (
    echo [ERRO] Arquivo triggers.xlsx não encontrado!
    echo Por favor, verifique se o arquivo existe na pasta do projeto.
    pause
    exit /b 1
)
echo [OK] Arquivo triggers.xlsx encontrado

REM Verificar se pasta de importação existe
set PASTA_IMPORTACAO=C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER
if not exist "%PASTA_IMPORTACAO%" (
    echo [AVISO] Pasta de importação não encontrada: %PASTA_IMPORTACAO%
    echo Criando pasta...
    mkdir "%PASTA_IMPORTACAO%" 2>nul
    if errorlevel 1 (
        echo [ERRO] Não foi possível criar a pasta.
        echo Por favor, crie manualmente e adicione os arquivos CSV.
        pause
        exit /b 1
    )
    echo [OK] Pasta criada.
)

REM Verificar se há arquivos CSV
dir /b "%PASTA_IMPORTACAO%\*.csv" >nul 2>&1
if errorlevel 1 (
    echo [AVISO] Nenhum arquivo CSV encontrado na pasta:
    echo %PASTA_IMPORTACAO%
    echo.
    echo Por favor, adicione os arquivos CSV e execute novamente.
    pause
    exit /b 1
)

REM Contar arquivos CSV
for /f %%i in ('dir /b "%PASTA_IMPORTACAO%\*.csv" 2^>nul ^| find /c /v ""') do set TOTAL_ARQUIVOS=%%i
echo [INFO] Encontrados %TOTAL_ARQUIVOS% arquivo(s) CSV para processar
echo.

REM Verificar dependências primeiro
echo Verificando instalação das dependências...
%PYTHON_CMD% verificar_instalacao.py >nul 2>&1
if errorlevel 1 (
    echo.
    echo [AVISO] Algumas dependências podem estar faltando.
    echo Instalando dependências automaticamente...
    echo.
    %PYTHON_CMD% -m pip install -q -r requirements.txt
    if errorlevel 1 (
        echo.
        echo [ERRO] Falha ao instalar dependências.
        echo Execute manualmente: instalar_dependencias.bat
        pause
        exit /b 1
    )
    echo [OK] Dependências instaladas.
    echo.
)

echo ============================================
echo Iniciando processamento dos arquivos
echo ============================================
echo Pasta: %PASTA_IMPORTACAO%
echo Arquivos encontrados: %TOTAL_ARQUIVOS%
echo.

%PYTHON_CMD% processar_arquivos_importacao.py
set RESULTADO=%ERRORLEVEL%

echo.
echo ============================================
if %RESULTADO% EQU 0 (
    echo Processamento concluído com sucesso!
    echo.
    echo Verifique:
    echo   - Logs: logs\qigger.log
    echo   - Banco de dados: data\portabilidade.db
) else (
    echo ERRO: Falha no processamento.
    echo.
    echo Verifique os erros acima e o log: logs\qigger.log
)
echo ============================================
echo.
pause

