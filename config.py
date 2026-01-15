"""
Configurações centralizadas do projeto 3F Qigger DB Gerenciador

Este arquivo contém todas as configurações de caminhos, conexões e parâmetros
utilizados pelos scripts de processamento.
"""

from pathlib import Path

# =============================================================================
# DIRETÓRIOS DO PROJETO
# =============================================================================

PROJECT_ROOT = Path(__file__).parent

# Banco de dados
DB_PATH = str(PROJECT_ROOT / "data" / "portabilidade.db")

# Pastas de entrada/saída
PASTA_ENTRADA = str(PROJECT_ROOT / "data" / "entrada")
PASTA_SAIDA = str(PROJECT_ROOT / "data" / "saida")
PASTA_LOGS = str(PROJECT_ROOT / "logs")

# Pasta de importações (arquivos externos)
PASTA_IMPORTACOES = "/Applications/Documentos/IMPORTACOES_QIGGER"

# =============================================================================
# CONFIGURAÇÃO SMB - COMPARTILHAMENTO DE REDE
# =============================================================================

# Servidor SMB
SMB_SERVER = "files"
SMB_SHARE = "02 Planejamento"

# Caminho para o arquivo COVERTE BASE PROP no servidor
SMB_COVERTE_PATH = "02 - Relatórios/08 - Relatorios Cliente"
SMB_COVERTE_FILE = "COVERTE BASE PROP.xlsx"

# URL SMB completa (para referência)
SMB_URL = f"smb://{SMB_SERVER}/{SMB_SHARE}/{SMB_COVERTE_PATH}/{SMB_COVERTE_FILE}"

# Ponto de montagem no macOS (onde o volume aparece quando montado)
SMB_MOUNT_POINT = f"/Volumes/{SMB_SHARE}"

# =============================================================================
# CAMINHOS DERIVADOS (calculados a partir das configurações acima)
# =============================================================================

# Caminho da pasta base na rede (quando montado)
PASTA_BASE_COVERTE_NETWORK = f"{SMB_MOUNT_POINT}/{SMB_COVERTE_PATH}"

# Caminho completo do arquivo na rede (quando montado)
ARQUIVO_BASE_COVERTE_NETWORK = f"{PASTA_BASE_COVERTE_NETWORK}/{SMB_COVERTE_FILE}"

# Caminho local para cópia do arquivo (fallback)
PASTA_BASE_COVERTE_LOCAL = str(PROJECT_ROOT / "data" / "entrada" / "excel")

# =============================================================================
# CONFIGURAÇÃO DE PROCESSAMENTO
# =============================================================================

# Tamanho de lote para commits no banco
BATCH_SIZE = 100

# Limite de registros para processamento (0 = sem limite)
MAX_REGISTROS = 0

# Formatos de data suportados (ordem de tentativa)
DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
]

# =============================================================================
# CONFIGURAÇÃO DE LOGGING
# =============================================================================

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_FILE_BACKUP_COUNT = 5

# =============================================================================
# FUNÇÕES UTILITÁRIAS
# =============================================================================

def criar_diretorios():
    """Cria os diretórios necessários se não existirem."""
    diretorios = [
        PASTA_ENTRADA,
        PASTA_SAIDA,
        PASTA_LOGS,
        PASTA_BASE_COVERTE_LOCAL,
    ]
    
    for diretorio in diretorios:
        Path(diretorio).mkdir(parents=True, exist_ok=True)


def verificar_conexao_rede() -> bool:
    """Verifica se o compartilhamento de rede está acessível."""
    return Path(SMB_MOUNT_POINT).exists() and Path(SMB_MOUNT_POINT).is_dir()


def obter_arquivo_coverte() -> Path | None:
    """
    Obtém o caminho do arquivo COVERTE BASE PROP.
    
    Verifica primeiro na rede, depois localmente.
    
    Returns:
        Path do arquivo ou None se não encontrado
    """
    # Tentar rede primeiro
    arquivo_rede = Path(ARQUIVO_BASE_COVERTE_NETWORK)
    if arquivo_rede.exists():
        return arquivo_rede
    
    # Tentar local
    pasta_local = Path(PASTA_BASE_COVERTE_LOCAL)
    if pasta_local.exists():
        arquivos = list(pasta_local.glob("COVERTE BASE PROP*.xlsx"))
        if arquivos:
            return max(arquivos, key=lambda x: x.stat().st_mtime)
    
    return None


# =============================================================================
# INICIALIZAÇÃO
# =============================================================================

# Criar diretórios ao importar este módulo
criar_diretorios()
