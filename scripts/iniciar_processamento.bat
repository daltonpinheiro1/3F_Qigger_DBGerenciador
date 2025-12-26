@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Iniciar Processamento
color 0E

echo ============================================
echo 3F Qigger DB Gerenciador
echo Sistema de Gerenciamento de Portabilidade
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

REM Mudar para o diretório raiz do projeto (pai de scripts/)
cd /d "%~dp0.."

:MENU
cls
echo ============================================
echo 3F Qigger DB Gerenciador
echo ============================================
echo.
echo Escolha uma opção:
echo.
echo 1. Iniciar Monitoramento Automático
echo 2. Processar Arquivo CSV
echo 3. Executar Exemplo
echo 4. Listar Regras
echo 5. Ver Ajuda
echo 6. Sair
echo.
set /p opcao="Digite o número da opção: "

if "%opcao%"=="1" goto :MONITORAMENTO
if "%opcao%"=="2" goto :PROCESSAR
if "%opcao%"=="3" goto :EXEMPLO
if "%opcao%"=="4" goto :REGRAS
if "%opcao%"=="5" goto :AJUDA
if "%opcao%"=="6" goto :SAIR

echo Opção inválida!
timeout /t 2 >nul
goto :MENU

:MONITORAMENTO
cls
echo Iniciando Monitoramento Automático...
echo Pressione Ctrl+C para parar.
echo.
%PYTHON_CMD% iniciar_monitoramento.py
pause
goto :MENU

:PROCESSAR
cls
echo Processar Arquivo CSV
echo.
set /p arquivo="Digite o caminho do arquivo CSV: "
if "%arquivo%"=="" (
    echo Arquivo não informado!
    timeout /t 2 >nul
    goto :MENU
)
echo.
set /p verbose="Exibir logs detalhados? (S/N): "
set CMD=%PYTHON_CMD% main.py --csv "%arquivo%"
if /i "%verbose%"=="S" set CMD=%CMD% --verbose
echo.
echo Processando...
%CMD%
pause
goto :MENU

:EXEMPLO
cls
echo Executando Exemplo...
echo.
%PYTHON_CMD% main.py --example
pause
goto :MENU

:REGRAS
cls
echo Listando Regras Disponíveis...
echo.
%PYTHON_CMD% main.py --list-rules
pause
goto :MENU

:AJUDA
cls
echo Ajuda - 3F Qigger DB Gerenciador
echo.
echo OPÇÕES:
echo.
echo 1. Monitoramento Automático
echo    - Monitora uma pasta e processa arquivos CSV automaticamente
echo    - Configurado no arquivo iniciar_monitoramento.py
echo.
echo 2. Processar Arquivo CSV
echo    - Processa um arquivo CSV específico
echo    - Suporta opções: --verbose, --keep-file, etc.
echo.
echo 3. Executar Exemplo
echo    - Executa um exemplo de processamento de registro
echo.
echo 4. Listar Regras
echo    - Mostra todas as 23 regras de decisão disponíveis
echo.
echo Para mais informações, execute: %PYTHON_CMD% main.py --help
echo.
pause
goto :MENU

:SAIR
exit /b 0

