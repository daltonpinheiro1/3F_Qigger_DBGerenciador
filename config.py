"""
Arquivo de configuração centralizado para 3F Qigger DB Gerenciador
Ajustado para Mac - caminhos locais sem nuvem
"""
import os
from pathlib import Path

# ========== CAMINHOS PRINCIPAIS (Mac) ==========

# Caminho base do projeto (relativo ao arquivo config.py)
PROJECT_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PROJECT_ROOT / "data"

# Banco de dados - usar caminho absoluto no Mac
DB_PATH = "/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db"

# Pasta de importações (arquivos CSV e XLSX de objetos)
PASTA_IMPORTACOES = Path("/Applications/Documentos/IMPORTACOES_QIGGER")

# Base COVERTE BASE PROP (Excel) - caminho de rede
# No Mac, quando montado via SMB, fica em /Volumes
# Caminho completo: /Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx
# Esta é uma tabela separada (base_coverte_prop) - NÃO é a base_unificada
# A base_unificada continua sendo para relatório de objetos e gerenciador
PASTA_BASE_COVERTE_NETWORK = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente")
ARQUIVO_BASE_COVERTE_NETWORK = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx")
PASTA_BASE_COVERTE_LOCAL = DATA_DIR / "entrada" / "excel"  # Fallback local

# Triggers (regras de decisão)
TRIGGERS_PATH = PROJECT_ROOT / "triggers.xlsx"

# ========== PASTAS DE TRABALHO ==========

# Pastas de entrada e saída (usando caminho absoluto)
PASTA_ENTRADA = DATA_DIR / "entrada"
PASTA_PROCESSADOS = DATA_DIR / "processados"
PASTA_ERROS = DATA_DIR / "erros"
PASTA_RETORNOS = DATA_DIR / "retornos"

# Subpastas de retornos
PASTA_RETORNOS_GOOGLE_DRIVE = PASTA_RETORNOS / "google_drive"
PASTA_RETORNOS_BACKOFFICE = PASTA_RETORNOS / "backoffice"

# Pasta de logs
PASTA_LOGS = PROJECT_ROOT / "logs"

# ========== ARQUIVOS DE SAÍDA ==========

# Pasta de saída para arquivos de homologação (retornos do gerenciador)
PASTA_SAIDA_HOMOLOGACAO = Path("/Applications/Documentos/Projetos_python/Retornos do gerenciador")

# Arquivos de homologação (usando caminho absoluto)
OUTPUT_WPP = DATA_DIR / "homologacao_wpp.csv"
OUTPUT_APROVISIONAMENTOS = DATA_DIR / "homologacao_aprovisionamentos.csv"
OUTPUT_IMPORTACAO = DATA_DIR / "importacao_final.csv"
OUTPUT_REABERTURA = DATA_DIR / "homologacao_reabertura.csv"

# WPP Output (Régua de Comunicação)
WPP_OUTPUT_PATH = DATA_DIR / "WPP_Regua_Output.csv"

# ========== CONFIGURAÇÕES DE PROCESSAMENTO ==========

# Deletar arquivo após processar? (True = deletar, False = manter)
DELETE_AFTER_PROCESS = True

# Tamanho do lote para processamento
BATCH_SIZE = 100

# Monitorar subpastas recursivamente?
RECURSIVE_MONITORING = True

# ========== CONFIGURAÇÕES DE LOG ==========

# Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = "INFO"

# ========== FUNÇÃO PARA CARREGAR CONFIGURAÇÕES ==========

def load_config():
    """
    Carrega configurações do arquivo config.py ou variáveis de ambiente
    
    Returns:
        Dicionário com todas as configurações
    """
    config = {
        # Caminhos principais
        'DB_PATH': os.getenv('QIGGER_DB_PATH', DB_PATH),
        'PASTA_IMPORTACOES': os.getenv('QIGGER_PASTA_IMPORTACOES', str(PASTA_IMPORTACOES)),
        'TRIGGERS_PATH': os.getenv('QIGGER_TRIGGERS_PATH', str(TRIGGERS_PATH)),
        
        # Pastas
        'PASTA_ENTRADA': os.getenv('QIGGER_PASTA_ENTRADA', str(PASTA_ENTRADA)),
        'PASTA_PROCESSADOS': os.getenv('QIGGER_PASTA_PROCESSADOS', str(PASTA_PROCESSADOS)),
        'PASTA_ERROS': os.getenv('QIGGER_PASTA_ERROS', str(PASTA_ERROS)),
        'PASTA_RETORNOS': os.getenv('QIGGER_PASTA_RETORNOS', str(PASTA_RETORNOS)),
        'PASTA_LOGS': os.getenv('QIGGER_PASTA_LOGS', str(PASTA_LOGS)),
        
        # Arquivos de saída
        'OUTPUT_WPP': os.getenv('QIGGER_OUTPUT_WPP', str(OUTPUT_WPP)),
        'OUTPUT_APROVISIONAMENTOS': os.getenv('QIGGER_OUTPUT_APROV', str(OUTPUT_APROVISIONAMENTOS)),
        'OUTPUT_IMPORTACAO': os.getenv('QIGGER_OUTPUT_IMPORT', str(OUTPUT_IMPORTACAO)),
        'WPP_OUTPUT_PATH': os.getenv('QIGGER_WPP_OUTPUT', str(WPP_OUTPUT_PATH)),
        
        # Configurações de processamento
        'DELETE_AFTER_PROCESS': os.getenv('QIGGER_DELETE_AFTER', str(DELETE_AFTER_PROCESS)).lower() == 'true',
        'BATCH_SIZE': int(os.getenv('QIGGER_BATCH_SIZE', BATCH_SIZE)),
        'RECURSIVE_MONITORING': os.getenv('QIGGER_RECURSIVE', str(RECURSIVE_MONITORING)).lower() == 'true',
        
        # Configurações de log
        'LOG_LEVEL': os.getenv('QIGGER_LOG_LEVEL', LOG_LEVEL),
    }
    
    return config


def criar_pastas_necessarias():
    """Cria todas as pastas necessárias se não existirem"""
    pastas = [
        PASTA_ENTRADA,
        PASTA_PROCESSADOS,
        PASTA_ERROS,
        PASTA_RETORNOS,
        PASTA_RETORNOS_GOOGLE_DRIVE,
        PASTA_RETORNOS_BACKOFFICE,
        PASTA_LOGS,
    ]
    
    for pasta in pastas:
        pasta.mkdir(parents=True, exist_ok=True)


# Criar pastas ao importar o módulo
criar_pastas_necessarias()

