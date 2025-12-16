@echo off
chcp 65001 >nul
title 3F Qigger DB Gerenciador - Preparação Completa
color 0B

echo ============================================
echo 3F Qigger DB Gerenciador
echo PREPARAÇÃO COMPLETA DO SISTEMA
echo ============================================
echo.

REM Verificar Python - tentar diferentes comandos
set PYTHON_CMD=
python --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python
    goto :python_found
)

py --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=py
    goto :python_found
)

python3 --version >nul 2>&1
if not errorlevel 1 (
    set PYTHON_CMD=python3
    goto :python_found
)

REM Python não encontrado
echo [ERRO] Python não encontrado!
echo.
echo Tentativas realizadas:
echo   - python (não encontrado)
echo   - py (não encontrado)
echo   - python3 (não encontrado)
echo.
echo SOLUÇÕES:
echo   1. Instale o Python 3.8 ou superior:
echo      Download: https://www.python.org/downloads/
echo.
echo   2. Durante a instalação, marque a opção:
echo      "Add Python to PATH"
echo.
echo   3. Após instalar, feche e reabra este terminal
echo.
pause
exit /b 1

:python_found
echo [OK] Python encontrado usando: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

REM Mudar para o diretório do script
cd /d "%~dp0"

REM Se PYTHON_CMD não foi definido, definir agora
if "%PYTHON_CMD%"=="" (
    python --version >nul 2>&1
    if not errorlevel 1 (
        set PYTHON_CMD=python
    ) else (
        py --version >nul 2>&1
        if not errorlevel 1 (
            set PYTHON_CMD=py
        ) else (
            set PYTHON_CMD=python3
        )
    )
)

echo ============================================
echo ETAPA 1: Verificando estrutura de pastas
echo ============================================
echo.

if not exist "data" (
    echo Criando pasta data...
    mkdir data
    echo [OK] Pasta data criada
) else (
    echo [OK] Pasta data existe
)

if not exist "logs" (
    echo Criando pasta logs...
    mkdir logs
    echo [OK] Pasta logs criada
) else (
    echo [OK] Pasta logs existe
)

if not exist "data\entrada" (
    echo Criando pasta data\entrada...
    mkdir data\entrada
    echo [OK] Pasta data\entrada criada
)

if not exist "data\processados" (
    echo Criando pasta data\processados...
    mkdir data\processados
    echo [OK] Pasta data\processados criada
)

if not exist "data\erros" (
    echo Criando pasta data\erros...
    mkdir data\erros
    echo [OK] Pasta data\erros criada
)

echo.
echo ============================================
echo ETAPA 2: Verificando instalação atual
echo ============================================
echo.

%PYTHON_CMD% verificar_instalacao.py
set VERIFICACAO=%ERRORLEVEL%

if %VERIFICACAO% NEQ 0 (
    echo.
    echo ============================================
    echo ETAPA 3: Instalando dependências
    echo ============================================
    echo.
    
    %PYTHON_CMD% -m pip install --upgrade pip
    echo.
    echo Instalando dependências do requirements.txt...
    %PYTHON_CMD% -m pip install -r requirements.txt
    
    if errorlevel 1 (
        echo.
        echo [ERRO] Falha ao instalar dependências.
        pause
        exit /b 1
    )
    
    echo.
    echo Verificando instalação novamente...
    %PYTHON_CMD% verificar_instalacao.py
    if errorlevel 1 (
        echo.
        echo [AVISO] Algumas dependências podem estar faltando.
        echo O sistema pode não funcionar corretamente.
    )
) else (
    echo.
    echo [OK] Todas as dependências estão instaladas.
)

echo.
echo ============================================
echo ETAPA 4: Verificando arquivos de importação
echo ============================================
echo.

set PASTA_IMPORTACAO=C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER

if exist "%PASTA_IMPORTACAO%" (
    echo [OK] Pasta de importação encontrada: %PASTA_IMPORTACAO%
    echo.
    echo Arquivos CSV encontrados:
    dir /b "%PASTA_IMPORTACAO%\*.csv" 2>nul
    if errorlevel 1 (
        echo   (nenhum arquivo CSV encontrado)
    )
) else (
    echo [AVISO] Pasta de importação não encontrada: %PASTA_IMPORTACAO%
    echo Criando pasta...
    mkdir "%PASTA_IMPORTACAO%" 2>nul
    if not errorlevel 1 (
        echo [OK] Pasta criada
    ) else (
        echo [ERRO] Não foi possível criar a pasta
    )
)

echo.
echo ============================================
echo PREPARAÇÃO CONCLUÍDA!
echo ============================================
echo.
echo Próximos passos:
echo   1. Para processar arquivos: processar_arquivos_importacao.bat
echo   2. Para iniciar monitoramento: iniciar_monitoramento.bat
echo   3. Para menu interativo: iniciar_processamento.bat
echo.
pause

