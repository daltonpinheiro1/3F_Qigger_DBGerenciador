"""
Script para iniciar o monitoramento com as configurações corretas
Versão 2.0 - Com suporte a triggers.xlsx
"""
import sys
import logging
from pathlib import Path

# Caminho para o arquivo de triggers
TRIGGERS_PATH = Path(__file__).parent / "triggers.xlsx"

# Tentar carregar configurações
try:
    from config import load_config
    config = load_config()
except ImportError:
    # Usar valores padrão se config.py não existir
    config = {
        'MONITOR_FOLDER': r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER",
        'GOOGLE_DRIVE_PATH': r"G:\Meu Drive\Retornos_Qigger",
        'BACKOFFICE_PATH': r"\\files\07 Backoffice\RETORNOS RPA - QIGGER\GERENCIAMENTO",
        'DB_PATH': "data/portabilidade.db",
        'TRIGGERS_PATH': str(TRIGGERS_PATH),
        'DELETE_AFTER_PROCESS': True,
        'BATCH_SIZE': 100,
        'RECURSIVE_MONITORING': True,
        'LOG_LEVEL': "INFO",
    }

# Configurar logging
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import io
if sys.platform == 'win32':
    try:
        console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
    except Exception:
        console_handler = logging.StreamHandler(sys.stdout)
else:
    console_handler = logging.StreamHandler(sys.stdout)

Path('logs').mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, config['LOG_LEVEL']),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/qigger.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Inicia o monitoramento"""
    # Determinar triggers_path
    triggers_path = config.get('TRIGGERS_PATH', str(TRIGGERS_PATH))
    
    logger.info("=" * 70)
    logger.info("3F Qigger DB Gerenciador - Monitoramento Automático")
    logger.info("Versão 2.0 - Com suporte a triggers.xlsx")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Configurações:")
    logger.info(f"  Monitor: {config['MONITOR_FOLDER']}")
    logger.info(f"  Triggers: {triggers_path}")
    logger.info(f"  Google Drive: {config['GOOGLE_DRIVE_PATH']}")
    logger.info(f"  Backoffice: {config['BACKOFFICE_PATH']}")
    logger.info(f"  Banco de dados: {config['DB_PATH']}")
    logger.info(f"  Deletar após processar: {config['DELETE_AFTER_PROCESS']}")
    logger.info(f"  Tamanho do lote: {config['BATCH_SIZE']}")
    logger.info("")
    
    # Verificar se triggers.xlsx existe
    if not Path(triggers_path).exists():
        logger.error(f"Arquivo triggers.xlsx não encontrado: {triggers_path}")
        logger.error("Por favor, verifique se o arquivo existe na pasta do projeto.")
        sys.exit(1)
    
    logger.info("Pressione Ctrl+C para parar o monitoramento...")
    logger.info("")
    
    # Verificar se pasta de monitoramento existe
    monitor_path = Path(config['MONITOR_FOLDER'])
    if not monitor_path.exists():
        logger.warning(f"Pasta de monitoramento não existe: {monitor_path}")
        logger.info(f"Criando pasta: {monitor_path}")
        monitor_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Pasta criada com sucesso")
    
    # Importar e iniciar monitor
    from src.monitor import FolderMonitor
    import time
    
    try:
        monitor = FolderMonitor(
            watch_folder=config['MONITOR_FOLDER'],
            db_path=config['DB_PATH'],
            google_drive_path=config['GOOGLE_DRIVE_PATH'],
            backoffice_path=config['BACKOFFICE_PATH'],
            delete_after_process=config['DELETE_AFTER_PROCESS'],
            recursive=config['RECURSIVE_MONITORING'],
            triggers_path=triggers_path
        )
        
        monitor.start()
        
        # Manter o programa rodando
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\nInterrompendo monitoramento...")
            monitor.stop()
            logger.info("Monitoramento encerrado.")
            
    except Exception as e:
        logger.error(f"Erro no monitoramento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()


