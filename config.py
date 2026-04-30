"""
Arquivo de configuração centralizado para 3F Qigger DB Gerenciador
Ajustado para Mac - caminhos locais sem nuvem
"""
import os
from pathlib import Path
from urllib.parse import quote

# Carregar variáveis do .env (credenciais SMB, etc.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ========== CAMINHOS PRINCIPAIS (Mac) ==========

# Caminho base do projeto (relativo ao arquivo config.py)
PROJECT_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PROJECT_ROOT / "data"

# Banco de dados - usar caminho absoluto no Mac
DB_PATH = "/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db"

# Novo banco de dados v2 (normalizado, versionado)
DB_V2_PATH = str(DATA_DIR / "portabilidade_v2.db")

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

# BS_VENDA_DU (Excel) - mesmo compartilhamento SMB: 02 Planejamento/08 - Relatorios Cliente
ARQUIVO_BS_VENDA_DU_NETWORK = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/BS_VENDA_DU.xlsx")
PASTA_BS_VENDA_DU_LOCAL = DATA_DIR / "entrada" / "excel"  # Fallback local

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

# ========== CONFIGURAÇÃO DE PROXIES (Reprocessamento de Endereços) ==========

# Arquivo com lista de proxies (um por linha, formato http://host:port)
# Usado pelo ProxyManager no reprocessamento de endereços inválidos
# Definir no .env: PROXY_FILE=data/proxies.txt ou deixar vazio para requisições diretas
PROXY_FILE = os.getenv("PROXY_FILE", str(DATA_DIR / "proxies.txt"))

# ========== ARQUIVOS DE SAÍDA ==========

# Pasta de saída para arquivos de homologação (retornos do gerenciador)
PASTA_SAIDA_HOMOLOGACAO = Path("/Applications/Documentos/Projetos_python/Retornos do gerenciador")

# Backup do banco na rede (SMB 07 Backoffice)
# Credenciais para montar conexão "files" (servidor SMB)
# Definir no .env: SMB_USER=3f\dalton.pinheiro  SMB_PASSWORD=sua_senha
SMB_USER = os.getenv("SMB_USER", "").strip()
SMB_PASSWORD = os.getenv("SMB_PASSWORD", "").strip()

def _smb_url(share: str) -> str:
    """URL SMB com credenciais se definidas no .env."""
    share_encoded = quote(share, safe="")
    if SMB_USER and SMB_PASSWORD:
        user_encoded = quote(SMB_USER, safe="")
        pass_encoded = quote(SMB_PASSWORD, safe="")
        return f"smb://{user_encoded}:{pass_encoded}@files/{share_encoded}"
    return f"smb://files/{share}"

# URL para montar 07 Backoffice (backup do banco)
SMB_URL_07_BACKOFFICE = _smb_url("07 Backoffice")
# Caminho local quando montado no macOS:
BACKUP_REDE_DIR = Path("/Volumes/07 Backoffice/RETORNOS RPA - QIGGER/db.Portabilidade")
BACKUP_REDE_PATH = str(BACKUP_REDE_DIR / "portabilidade.db")

# Arquivos de homologação (usando caminho absoluto)
OUTPUT_WPP = DATA_DIR / "homologacao_wpp.xlsx"
OUTPUT_APROVISIONAMENTOS = DATA_DIR / "homologacao_aprovisionamentos.csv"
OUTPUT_IMPORTACAO = DATA_DIR / "importacao_final.csv"
OUTPUT_REABERTURA = DATA_DIR / "homologacao_reabertura.csv"

# WPP Output (Régua de Comunicação)
WPP_OUTPUT_PATH = DATA_DIR / "WPP_Regua_Output.csv"

# ========== CONFIGURAÇÃO EVA (SQL Server) ==========

EVA_SERVER = os.getenv("EVA_SERVER", "3fdb.vexten.com.br")
EVA_DATABASE = os.getenv("EVA_DATABASE", "eva_activities")
EVA_USER = os.getenv("EVA_USER", "mis")
EVA_PASSWORD = os.getenv("EVA_PASSWORD", "")
EVA_VIEW = "eva_activities.dbo.vwSales"

# Tabela de parametrização de status (lista ordenada — primeira regra vence)
PARAMETRIZACAO_STATUS = [
    {"padrao": "O plano: TIM Controle", "status": "CLIENTE JÁ MIGRADO"},
    {"padrao": "Score insuficiente, score:", "status": "LIMITE DE CREDITO"},
    {"padrao": "[Sistema] Não foi possível processar esse registro.", "status": "FALHA PROCESSAMENTO"},
    {"padrao": "Existe uma ordem Em aprovisionamento", "status": "ORDEM EM PROCESSAMENTO"},
    {"padrao": "Perfil Pré-Pago. Cliente com pendência financeira na TIM", "status": "PENDENCIA FINANCEIRA"},
    {"padrao": "Perfil Pré-Pago. Cliente com restrição de administrativa na TIM", "status": "RESTRICAO INTERNA"},
    {"padrao": "O numero de acesso se encontra Cancelado.", "status": "LINHA INATIVA"},
    {"padrao": "Cliente não possui nenhum Billing Profile com endereço correspondente ao DDD selecionado", "status": "DDD FORA DO ESTADO"},
    {"padrao": "Endereço divergente", "status": "DIVERGENCIA ENDERECO"},
    {"padrao": "Erro ao executar o sub-processo 'TIM Criar Cliente WF'", "status": "FALHA PROCESSAMENTO"},
    {"padrao": "CEP não encontrado", "status": "DIVERGENCIA ENDERECO"},
    {"padrao": "Erro de comunicação com o Crivo", "status": "FALHA PROCESSAMENTO"},
    {"padrao": "Nome da mãe deve conter apenas letras", "status": "DADOS CADASTRAIS INVALIDOS"},
]

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
        'DB_V2_PATH': os.getenv('QIGGER_DB_V2_PATH', DB_V2_PATH),
        'PASTA_IMPORTACOES': os.getenv('QIGGER_PASTA_IMPORTACOES', str(PASTA_IMPORTACOES)),
        'TRIGGERS_PATH': os.getenv('QIGGER_TRIGGERS_PATH', str(TRIGGERS_PATH)),
        'PROXY_FILE': os.getenv('PROXY_FILE', PROXY_FILE),
        
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
        'BATCH_SIZE': int(os.getenv('QIGGER_BATCH_SIZE', str(BATCH_SIZE))),
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

