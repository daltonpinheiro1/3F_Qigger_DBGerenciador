"""
Script para processar todos os arquivos CSV na pasta de importação
"""
import sys
import logging
from pathlib import Path
from typing import List

# Configurar encoding UTF-8 para o console no Windows
from src.utils.console_utils import setup_windows_console
setup_windows_console()

# Configurar logging
import io

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
        logging.FileHandler('logs/qigger.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.utils import CSVParser
from src.utils.file_output_manager import FileOutputManager

def processar_arquivos_importacao():
    """Processa todos os arquivos CSV na pasta de importação"""
    
    pasta_importacao = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
    
    logger.info("=" * 70)
    logger.info("3F Qigger DB Gerenciador - Processamento de Arquivos")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Pasta de importação: {pasta_importacao}")
    
    # Verificar se pasta existe
    if not pasta_importacao.exists():
        logger.error(f"Pasta não encontrada: {pasta_importacao}")
        logger.info("Criando pasta...")
        pasta_importacao.mkdir(parents=True, exist_ok=True)
        logger.info("Pasta criada. Por favor, adicione os arquivos CSV e execute novamente.")
        return
    
    # Buscar arquivos CSV
    arquivos_csv = list(pasta_importacao.glob("*.csv"))
    
    if not arquivos_csv:
        logger.warning("Nenhum arquivo CSV encontrado na pasta de importação.")
        return
    
    logger.info(f"Encontrados {len(arquivos_csv)} arquivo(s) CSV:")
    for arquivo in arquivos_csv:
        logger.info(f"  - {arquivo.name}")
    logger.info("")
    
    # Inicializar componentes
    db_path = "data/portabilidade.db"
    db_manager = DatabaseManager(db_path)
    engine = QiggerDecisionEngine(db_manager)
    
    # Configurar paths de saída
    # Criar pastas locais para retornos
    pasta_base = Path(__file__).parent
    pasta_retornos = pasta_base / "data" / "retornos"
    pasta_retornos.mkdir(parents=True, exist_ok=True)
    
    # Paths locais (sempre criados)
    google_drive_path = str(pasta_retornos / "google_drive")
    backoffice_path = str(pasta_retornos / "backoffice")
    
    # Criar pastas se não existirem
    Path(google_drive_path).mkdir(parents=True, exist_ok=True)
    Path(backoffice_path).mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Pasta de retornos Google Drive: {google_drive_path}")
    logger.info(f"Pasta de retornos Backoffice: {backoffice_path}")
    logger.info("")
    
    # Configurar output manager para sempre gerar os arquivos
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
            
            # Gerenciar saída - SEMPRE gerar arquivos de retorno
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
            else:
                logger.warning("⚠ Nenhum arquivo de retorno foi gerado.")
            
            # Arquivo fonte mantido na pasta de importação (não deletado)
            logger.info(f"✓ Arquivo fonte mantido: {arquivo_csv}")
            
            # Informar caminhos importantes
            db_abs_path = str(Path(db_path).absolute())
            logger.info("")
            logger.info("=" * 70)
            logger.info("INFORMAÇÕES IMPORTANTES:")
            logger.info("=" * 70)
            logger.info(f"Banco de dados: {db_abs_path}")
            logger.info(f"Retornos Google Drive: {google_drive_path}")
            logger.info(f"Retornos Backoffice: {backoffice_path}")
            logger.info("=" * 70)
            
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
    logger.info("=" * 70)

if __name__ == "__main__":
    try:
        processar_arquivos_importacao()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

