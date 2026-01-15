"""
Script para processar arquivos CSV da pasta data/entrada/
Processa todos os arquivos CSV encontrados na pasta de entrada
"""
import sys
import logging
from pathlib import Path

# Configurar encoding UTF-8 para o console
from src.utils.console_utils import setup_windows_console
setup_windows_console()

# Configurar logging
import io

Path('logs').mkdir(exist_ok=True)

if sys.platform == 'win32':
    try:
        console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
    except Exception:
        console_handler = logging.StreamHandler(sys.stdout)
else:
    console_handler = logging.StreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/processar_arquivo.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.utils import CSVParser, ObjectsLoader
from src.utils.file_output_manager import FileOutputManager

# Caminhos de configuração
TRIGGERS_PATH = Path(__file__).parent / "triggers.xlsx"
PASTA_ENTRADA = Path(__file__).parent / "data" / "entrada"
PASTA_PROCESSADOS = Path(__file__).parent / "data" / "processados"
DB_PATH = "data/portabilidade.db"

def processar_arquivos_entrada():
    """Processa todos os arquivos CSV na pasta data/entrada/"""
    
    logger.info("=" * 70)
    logger.info("3F Qigger DB Gerenciador - Processar Arquivos")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Pasta de entrada: {PASTA_ENTRADA}")
    logger.info(f"Arquivo de triggers: {TRIGGERS_PATH}")
    
    # Verificar se triggers.xlsx existe
    if not TRIGGERS_PATH.exists():
        logger.error(f"Arquivo triggers.xlsx não encontrado: {TRIGGERS_PATH}")
        logger.error("Por favor, verifique se o arquivo existe na pasta do projeto.")
        return
    
    # Verificar se pasta existe
    if not PASTA_ENTRADA.exists():
        logger.info(f"Pasta não encontrada. Criando: {PASTA_ENTRADA}")
        PASTA_ENTRADA.mkdir(parents=True, exist_ok=True)
    
    # Criar pasta de processados
    PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
    
    # Buscar arquivos CSV
    arquivos_csv = list(PASTA_ENTRADA.glob("*.csv"))
    
    if not arquivos_csv:
        logger.warning("Nenhum arquivo CSV encontrado na pasta de entrada.")
        logger.info(f"Por favor, adicione arquivos CSV em: {PASTA_ENTRADA}")
        return
    
    logger.info(f"Encontrados {len(arquivos_csv)} arquivo(s) CSV:")
    for arquivo in arquivos_csv:
        logger.info(f"  - {arquivo.name}")
    logger.info("")
    
    # Inicializar componentes
    db_manager = DatabaseManager(DB_PATH)
    
    engine = QiggerDecisionEngine(
        db_manager, 
        triggers_path=str(TRIGGERS_PATH)
    )
    
    # Exibir estatísticas das regras
    stats = engine.get_rules_stats()
    logger.info(f"Regras carregadas: {stats['total_regras']}")
    logger.info("")
    
    # Configurar output manager
    pasta_base = Path(__file__).parent
    pasta_retornos = pasta_base / "data" / "retornos"
    pasta_retornos.mkdir(parents=True, exist_ok=True)
    
    google_drive_path = str(pasta_retornos / "google_drive")
    backoffice_path = str(pasta_retornos / "backoffice")
    
    Path(google_drive_path).mkdir(parents=True, exist_ok=True)
    Path(backoffice_path).mkdir(parents=True, exist_ok=True)
    
    output_manager = FileOutputManager(
        google_drive_path=google_drive_path,
        backoffice_path=backoffice_path
    )
    
    # Processar cada arquivo
    total_arquivos = len(arquivos_csv)
    total_processados = 0
    total_erros = 0
    
    for idx, arquivo_csv in enumerate(arquivos_csv, 1):
        logger.info("=" * 70)
        logger.info(f"Processando arquivo {idx}/{total_arquivos}: {arquivo_csv.name}")
        logger.info("=" * 70)
        logger.info("")
        
        try:
            # Parse do CSV
            records = CSVParser.parse_file(str(arquivo_csv))
            logger.info(f"Total de registros parseados: {len(records)}")
            
            if not records:
                logger.warning("Nenhum registro válido encontrado no arquivo.")
                # Mover para processados mesmo assim
                try:
                    destino = PASTA_PROCESSADOS / arquivo_csv.name
                    arquivo_csv.rename(destino)
                    logger.info(f"Arquivo movido para: {destino}")
                except Exception as e:
                    logger.warning(f"Erro ao mover arquivo: {e}")
                continue
            
            # Processar registros
            total_registros_arquivo = len(records)
            registros_processados = 0
            erros_arquivo = 0
            results_map = {}
            
            for i, record in enumerate(records, 1):
                try:
                    results = engine.process_record(record)
                    
                    # Armazenar resultados
                    key = f"{record.cpf}_{record.numero_ordem}"
                    results_map[key] = results
                    
                    registros_processados += 1
                    
                    if i % 100 == 0:
                        logger.info(f"  Progresso: {i}/{total_registros_arquivo} registros processados...")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar registro {i}: {e}")
                    erros_arquivo += 1
            
            logger.info("")
            logger.info(f"Arquivo processado: {registros_processados} registros, {erros_arquivo} erros")
            
            # Estatísticas
            mapeados = sum(1 for r in records if r.mapeado)
            nao_mapeados = len(records) - mapeados
            com_logistica = sum(1 for r in records if r.nome_cliente)
            com_template = sum(1 for r in records if r.template)
            
            logger.info(f"  Registros mapeados: {mapeados}")
            logger.info(f"  Registros não mapeados: {nao_mapeados}")
            logger.info(f"  Registros com dados de logística: {com_logistica}")
            logger.info(f"  Registros com Template (para WPP): {com_template}")
            
            # Gerenciar saída
            success = erros_arquivo == 0
            result = output_manager.process_and_cleanup(
                arquivo_csv,
                success=success,
                records=records,
                results_map=results_map
            )
            
            if result['copied_to']:
                logger.info(f"✓ Planilhas geradas/copiadas para {len(result['copied_to'])} destino(s):")
                for path in result['copied_to']:
                    logger.info(f"  → {path}")
            
            # Mover arquivo para pasta de processados
            if success and registros_processados > 0:
                try:
                    destino = PASTA_PROCESSADOS / arquivo_csv.name
                    arquivo_csv.rename(destino)
                    logger.info(f"✓ Arquivo movido para: {destino}")
                except Exception as e:
                    logger.warning(f"⚠ Não foi possível mover arquivo: {e}")
            
            total_processados += 1
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo_csv.name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            total_erros += 1
        
        logger.info("")
    
    # Resumo final
    logger.info("=" * 70)
    logger.info("RESUMO DO PROCESSAMENTO")
    logger.info("=" * 70)
    logger.info(f"Total de arquivos: {total_arquivos}")
    logger.info(f"Arquivos processados com sucesso: {total_processados}")
    logger.info(f"Arquivos com erro: {total_erros}")
    logger.info(f"Pasta de processados: {PASTA_PROCESSADOS}")
    logger.info("=" * 70)

if __name__ == "__main__":
    try:
        processar_arquivos_entrada()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

