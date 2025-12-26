@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Gerar Homologação WPP
color 0B

echo ============================================
echo 3F Qigger DB Gerenciador v2.0
echo Gerador de Homologação WPP
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
echo Por favor, instale o Python ou configure o PATH.
echo Execute: verificar_python.bat para verificar a instalação
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
echo.

REM Verificar dependências
echo Verificando dependências...
%PYTHON_CMD% verificar_instalacao.py >nul 2>&1
if errorlevel 1 (
    echo [AVISO] Algumas dependências podem estar faltando.
    echo Instalando dependências automaticamente...
    %PYTHON_CMD% -m pip install -q -r requirements.txt
    if errorlevel 1 (
        echo [ERRO] Falha ao instalar dependências.
        pause
        exit /b 1
    )
    echo [OK] Dependências instaladas.
    echo.
)

echo ============================================
echo Iniciando geração de homologação WPP
echo ============================================
echo.

REM Executar o script
%PYTHON_CMD% gerar_homologacao_wpp.py
set RESULTADO=%ERRORLEVEL%

echo.
echo ============================================
if %RESULTADO% EQU 0 (
    echo Homologação gerada com sucesso!
    echo.
    echo Arquivo gerado: data\homologacao_wpp.csv
    echo.
    echo Próximos passos:
    echo   1. Abra o arquivo CSV gerado
    echo   2. Revise a coluna 'Mensagem_Preview'
    echo   3. Valide os dados do cliente
    echo   4. Após homologação, use para envio real
) else (
    echo ERRO: Falha ao gerar homologação.
    echo.
    echo Verifique os erros acima e o log: logs\homologacao_wpp.log
)
echo ============================================
echo.
pause

