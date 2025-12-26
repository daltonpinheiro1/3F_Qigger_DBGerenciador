@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Processar Arquivo
color 0B

echo ============================================
echo 3F Qigger DB Gerenciador v2.0
echo Processar Arquivo CSV
echo Com suporte a triggers.xlsx
echo ============================================
echo.

REM Verificar se Python está instalado
python --version >nul 2>&1
if errorlevel 1 (
    echo ERRO: Python não encontrado! Por favor, instale o Python.
    pause
    exit /b 1
)

REM Mudar para o diretório raiz do projeto (pai de scripts/)
cd /d "%~dp0.."

REM Verificar se triggers.xlsx existe
if not exist "triggers.xlsx" (
    echo ERRO: Arquivo triggers.xlsx não encontrado!
    echo Por favor, verifique se o arquivo existe na pasta do projeto.
    pause
    exit /b 1
)

REM Verificar se arquivo foi passado como parâmetro
if "%~1"=="" (
    echo Uso: processar_arquivo.bat "caminho\para\arquivo.csv" [opções]
    echo.
    echo Opções disponíveis:
    echo   --verbose                    : Exibir logs detalhados
    echo   --keep-file                  : Manter arquivo após processamento
    echo   --google-drive "caminho"     : Caminho do Google Drive
    echo   --backoffice "caminho"       : Caminho do Backoffice
    echo   --move-processed "pasta"     : Mover para pasta após processar
    echo.
    echo Exemplo:
    echo   processar_arquivo.bat "data\entrada\arquivo.csv" --verbose
    echo.
    pause
    exit /b 1
)

echo Processando arquivo: %~1
echo.

REM Construir comando
set CMD=python main.py --csv "%~1"

REM Adicionar argumentos opcionais
:args
shift
if "%~1"=="" goto :process
set CMD=%CMD% %~1
goto :args

:process
REM Executar processamento
%CMD%

if errorlevel 1 (
    echo.
    echo ERRO: Falha ao processar arquivo.
    pause
    exit /b 1
)

echo.
echo Processamento concluído com sucesso!
pause

