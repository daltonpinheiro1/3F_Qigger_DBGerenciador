"""
Arquivo de configuração de exemplo
Copie este arquivo para config.py e ajuste os caminhos conforme necessário
"""
import os

# ========== CONFIGURAÇÕES DE PASTAS ==========

# Pasta monitorada (onde os arquivos CSV serão depositados)
MONITOR_FOLDER = r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER"

# Google Drive (onde os arquivos processados serão copiados)
GOOGLE_DRIVE_PATH = r"G:\Meu Drive\Retornos_Qigger"

# Backoffice (rede compartilhada onde os arquivos processados serão copiados)
BACKOFFICE_PATH = r"\\files\07 Backoffice\RETORNOS RPA - QIGGER\GERENCIAMENTO"

# Banco de dados
DB_PATH = "data/portabilidade.db"

# Pasta de logs
LOG_FOLDER = "logs"

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
    """
    config = {
        'MONITOR_FOLDER': os.getenv('QIGGER_MONITOR_FOLDER', MONITOR_FOLDER),
        'GOOGLE_DRIVE_PATH': os.getenv('QIGGER_GOOGLE_DRIVE', GOOGLE_DRIVE_PATH),
        'BACKOFFICE_PATH': os.getenv('QIGGER_BACKOFFICE', BACKOFFICE_PATH),
        'DB_PATH': os.getenv('QIGGER_DB_PATH', DB_PATH),
        'LOG_FOLDER': os.getenv('QIGGER_LOG_FOLDER', LOG_FOLDER),
        'DELETE_AFTER_PROCESS': os.getenv('QIGGER_DELETE_AFTER', str(DELETE_AFTER_PROCESS)).lower() == 'true',
        'BATCH_SIZE': int(os.getenv('QIGGER_BATCH_SIZE', BATCH_SIZE)),
        'RECURSIVE_MONITORING': os.getenv('QIGGER_RECURSIVE', str(RECURSIVE_MONITORING)).lower() == 'true',
        'LOG_LEVEL': os.getenv('QIGGER_LOG_LEVEL', LOG_LEVEL),
    }
    
    return config


