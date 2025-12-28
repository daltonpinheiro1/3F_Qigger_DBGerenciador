@echo off
chcp 65001 >nul
echo ======================================================================
echo GERAR ARQUIVO DE HOMOLOGAÇÃO - APROVISIONAMENTOS
echo ======================================================================
echo.

cd /d "%~dp0\.."

REM Detectar Python
set PYTHON_CMD=
where python >nul 2>&1 && set PYTHON_CMD=python
if not defined PYTHON_CMD where py >nul 2>&1 && set PYTHON_CMD=py
if not defined PYTHON_CMD where python3 >nul 2>&1 && set PYTHON_CMD=python3

if not defined PYTHON_CMD (
    echo ERRO: Python não encontrado!
    echo Instale Python 3.8 ou superior.
    pause
    exit /b 1
)

echo Python detectado: %PYTHON_CMD%
echo.

REM Verificar se requirements estão instalados
echo Verificando dependências...
%PYTHON_CMD% -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Instalando dependências...
    %PYTHON_CMD% -m pip install -r requirements.txt --quiet
)

echo.
echo Executando script de homologação...
echo.

%PYTHON_CMD% gerar_homologacao_aprovisionamentos.py

if errorlevel 1 (
    echo.
    echo ERRO ao gerar arquivo de homologação!
    pause
    exit /b 1
)

echo.
echo ======================================================================
echo HOMOLOGAÇÃO GERADA COM SUCESSO!
echo ======================================================================
echo Arquivo: data\homologacao_aprovisionamentos.csv
echo.
echo Próximo passo: Execute validar_aprovisionamentos.py para validar
echo ======================================================================
echo.
pause

