"""
3F Qigger DB Gerenciador - Sistema de Gerenciamento de Portabilidade
Arquivo principal com exemplo de uso
"""
import logging
import sys
import time
from pathlib import Path
from typing import List

from src.engine import QiggerDecisionEngine, DecisionResult
from src.database import DatabaseManager
from src.utils import CSVParser
from src.models.portabilidade import PortabilidadeRecord
from src.monitor import FolderMonitor

# Configurar encoding UTF-8 para o console no Windows
from src.utils.console_utils import setup_windows_console
setup_windows_console()

# Configurar logging
import io
import sys

# Configurar encoding UTF-8 para o console no Windows
if sys.platform == 'win32':
    # Criar handler com encoding UTF-8
    try:
        console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
    except Exception:
        # Fallback para handler padrão
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


def process_csv_file(
    csv_path: str, 
    db_path: str = "data/portabilidade.db",
    processed_folder: str = None,
    verbose: bool = False,
    batch_size: int = 100
):
    """
    Processa um arquivo CSV completo com otimizações de performance
    
    Args:
        csv_path: Caminho para o arquivo CSV
        db_path: Caminho para o banco de dados
        processed_folder: Pasta para mover arquivo após processamento (opcional)
        verbose: Exibir logs detalhados de cada registro
        batch_size: Tamanho do lote para processamento em batch
    """
    import shutil
    from pathlib import Path
    from datetime import datetime
    
    logger.info(f"Iniciando processamento do arquivo: {csv_path}")
    
    # Inicializar componentes
    db_manager = DatabaseManager(db_path)
    engine = QiggerDecisionEngine(db_manager)
    
    # Parse do CSV
    try:
        records = CSVParser.parse_file(csv_path)
        logger.info(f"Total de registros parseados: {len(records)}")
    except Exception as e:
        logger.error(f"Erro ao parsear CSV: {e}")
        return
    
    # Processar registros em lotes para melhor performance
    total_processed = 0
    total_errors = 0
    csv_path_obj = Path(csv_path)
    
    # Processar em lotes
    for batch_start in range(0, len(records), batch_size):
        batch_end = min(batch_start + batch_size, len(records))
        batch = records[batch_start:batch_end]
        
        logger.info(f"Processando lote {batch_start + 1}-{batch_end} de {len(records)} registros...")
        
        try:
            # Processar lote de forma otimizada
            if batch_size > 1:
                results_map = engine.process_records_batch(batch)
                
                # Processar resultados
                for i, (record, results) in enumerate(results_map.items(), start=batch_start + 1):
                    if verbose:
                        logger.info(f"Registro {i}/{len(records)}: CPF {record.cpf}, Ordem {record.numero_ordem}")
                    
                    # Exibir resultados apenas se verbose ou se houver regras aplicadas importantes
                    if verbose and results:
                        high_priority = [r for r in results if r.priority <= 2]
                        if high_priority:
                            logger.info(f"  >> {len(high_priority)} regra(s) de alta prioridade:")
                            for result in high_priority:
                                logger.info(f"    - {result.rule_name}: {result.decision}")
                    
                    total_processed += 1
            else:
                # Processar individualmente se batch_size = 1
                for i, record in enumerate(batch, start=batch_start + 1):
                    try:
                        if verbose:
                            logger.info(f"Processando registro {i}/{len(records)}: CPF {record.cpf}, Ordem {record.numero_ordem}")
                        
                        results = engine.process_record(record)
                        
                        if verbose and results:
                            high_priority = [r for r in results if r.priority <= 2]
                            if high_priority:
                                logger.info(f"  >> {len(high_priority)} regra(s) de alta prioridade:")
                                for result in high_priority:
                                    logger.info(f"    - {result.rule_name}: {result.decision}")
                        
                        total_processed += 1
                    except Exception as e:
                        logger.error(f"Erro ao processar registro {i}: {e}")
                        total_errors += 1
                        
        except Exception as e:
            logger.error(f"Erro ao processar lote {batch_start + 1}-{batch_end}: {e}")
            # Fallback: processar individualmente
            for i, record in enumerate(batch, start=batch_start + 1):
                try:
                    results = engine.process_record(record)
                    total_processed += 1
                except Exception as e2:
                    logger.error(f"Erro ao processar registro {i}: {e2}")
                    total_errors += 1
        
        # Log de progresso a cada lote
        logger.info(f"Progresso: {batch_end}/{len(records)} registros processados ({batch_end*100//len(records)}%)")
    
    logger.info(f"\nProcessamento concluído!")
    logger.info(f"  Total processado: {total_processed}")
    logger.info(f"  Total de erros: {total_errors}")
    
    # Mover arquivo para pasta de processados se especificado
    if processed_folder:
        try:
            processed_path = Path(processed_folder)
            processed_path.mkdir(parents=True, exist_ok=True)
            
            # Criar nome único com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{csv_path_obj.stem}_{timestamp}{csv_path_obj.suffix}"
            destination = processed_path / new_name
            
            shutil.move(str(csv_path_obj), str(destination))
            logger.info(f"Arquivo movido para: {destination}")
        except Exception as e:
            logger.warning(f"Não foi possível mover arquivo para pasta processados: {e}")


def process_single_record_example():
    """Exemplo de processamento de um único registro"""
    logger.info("=== Exemplo: Processamento de Registro Único ===\n")
    
    # Criar banco de dados e engine
    db_manager = DatabaseManager("data/portabilidade.db")
    engine = QiggerDecisionEngine(db_manager)
    
    # Criar um registro de exemplo
    from datetime import datetime
    from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
    
    record = PortabilidadeRecord(
        cpf="12345678901",
        numero_acesso="11987654321",
        numero_ordem="1-1234567890123",
        codigo_externo="250001234",
        status_bilhete=PortabilidadeStatus.CANCELADA,
        operadora_doadora="VIVO",
        data_portabilidade=datetime(2025, 12, 10, 14, 0, 0),
        motivo_cancelamento="Cancelamento pelo Cliente",
        status_ordem=StatusOrdem.CONCLUIDO,
        preco_ordem="R$29,99",
        registro_valido=True,
        numero_acesso_valido=True
    )
    
    # Processar registro
    logger.info(f"Processando registro:")
    logger.info(f"  CPF: {record.cpf}")
    logger.info(f"  Número de acesso: {record.numero_acesso}")
    logger.info(f"  Status do bilhete: {record.status_bilhete.value if record.status_bilhete else 'N/A'}")
    logger.info(f"  Status da ordem: {record.status_ordem.value if record.status_ordem else 'N/A'}\n")
    
    results = engine.process_record(record)
    
    # Exibir resultados
    logger.info(f"Resultados do processamento ({len(results)} regra(s) aplicada(s)):\n")
    for i, result in enumerate(results, 1):
        logger.info(f"{i}. {result.rule_name}")
        logger.info(f"   Decisão: {result.decision}")
        logger.info(f"   Ação: {result.action}")
        logger.info(f"   Detalhes: {result.details}")
        logger.info(f"   Prioridade: {result.priority}")
        logger.info(f"   Tempo de execução: {result.execution_time_ms:.2f}ms\n")


def list_all_rules():
    """Lista todas as regras disponíveis"""
    logger.info("=== Regras Disponíveis na QiggerDecisionEngine ===\n")
    
    engine = QiggerDecisionEngine()
    
    for i, rule_name in enumerate(engine.rules_registry.keys(), 1):
        logger.info(f"{i:2d}. {rule_name}")


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="3F Qigger DB Gerenciador - Sistema de Gerenciamento de Portabilidade"
    )
    
    parser.add_argument(
        '--csv',
        type=str,
        help='Caminho para arquivo CSV a ser processado'
    )
    
    parser.add_argument(
        '--db',
        type=str,
        default='data/portabilidade.db',
        help='Caminho para o banco de dados (padrão: data/portabilidade.db)'
    )
    
    parser.add_argument(
        '--example',
        action='store_true',
        help='Executar exemplo de processamento de registro único'
    )
    
    parser.add_argument(
        '--list-rules',
        action='store_true',
        help='Listar todas as regras disponíveis'
    )
    
    parser.add_argument(
        '--watch',
        type=str,
        help='Monitorar pasta para processamento automático de arquivos CSV'
    )
    
    parser.add_argument(
        '--processed-folder',
        type=str,
        help='Pasta para mover arquivos processados (usado com --watch)'
    )
    
    parser.add_argument(
        '--error-folder',
        type=str,
        help='Pasta para mover arquivos com erro (usado com --watch)'
    )
    
    parser.add_argument(
        '--no-recursive',
        action='store_true',
        help='Não monitorar subpastas recursivamente (usado com --watch)'
    )
    
    parser.add_argument(
        '--move-processed',
        type=str,
        help='Pasta para mover arquivo CSV após processamento (usado com --csv)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Exibir logs detalhados de cada registro processado'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Tamanho do lote para processamento (padrão: 100)'
    )
    
    args = parser.parse_args()
    
    # Criar diretório de logs
    Path('logs').mkdir(exist_ok=True)
    
    if args.list_rules:
        list_all_rules()
    elif args.example:
        process_single_record_example()
    elif args.watch:
        # Modo monitoramento de pasta
        logger.info("=== Modo Monitoramento de Pasta ===\n")
        logger.info(f"Monitorando pasta: {args.watch}")
        logger.info(f"Banco de dados: {args.db}")
        if args.processed_folder:
            logger.info(f"Pasta de processados: {args.processed_folder}")
        if args.error_folder:
            logger.info(f"Pasta de erros: {args.error_folder}")
        logger.info("\nPressione Ctrl+C para parar o monitoramento...\n")
        
        try:
            monitor = FolderMonitor(
                watch_folder=args.watch,
                db_path=args.db,
                processed_folder=args.processed_folder,
                error_folder=args.error_folder,
                recursive=not args.no_recursive
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
            sys.exit(1)
    elif args.csv:
        process_csv_file(
            args.csv, 
            args.db,
            processed_folder=args.move_processed,
            verbose=args.verbose,
            batch_size=args.batch_size
        )
    else:
        # Modo interativo
        logger.info("=== 3F Qigger DB Gerenciador ===\n")
        logger.info("Opções disponíveis:")
        logger.info("  1. Processar arquivo CSV: python main.py --csv <caminho>")
        logger.info("  2. Monitorar pasta: python main.py --watch <pasta>")
        logger.info("  3. Executar exemplo: python main.py --example")
        logger.info("  4. Listar regras: python main.py --list-rules")
        logger.info("\nUse --help para mais informações.")


if __name__ == "__main__":
    main()

