"""
Script para processar todos os arquivos CSV na pasta de importação
Versão 3.0 - Com suporte a triggers.xlsx + integração logística + Régua WPP
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
        logging.FileHandler('logs/qigger.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.utils import CSVParser, ObjectsLoader, WPPOutputGenerator
from src.utils.file_output_manager import FileOutputManager

# Caminhos de configuração
TRIGGERS_PATH = Path(__file__).parent / "triggers.xlsx"
PASTA_IMPORTACAO = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
WPP_OUTPUT_PATH = Path(r"G:\Meu Drive\3F Contact Center\WPP_Regua_Output.csv")

def processar_arquivos_importacao():
    """Processa todos os arquivos CSV na pasta de importação"""
    
    logger.info("=" * 70)
    logger.info("3F Qigger DB Gerenciador - Processamento de Arquivos")
    logger.info("Versão 3.0 - triggers.xlsx + logística + Régua WPP")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Pasta de importação: {PASTA_IMPORTACAO}")
    logger.info(f"Arquivo de triggers: {TRIGGERS_PATH}")
    logger.info(f"Saída WPP: {WPP_OUTPUT_PATH}")
    
    # Verificar se triggers.xlsx existe
    if not TRIGGERS_PATH.exists():
        logger.error(f"Arquivo triggers.xlsx não encontrado: {TRIGGERS_PATH}")
        logger.error("Por favor, verifique se o arquivo existe na pasta do projeto.")
        return
    
    # Verificar se pasta existe
    if not PASTA_IMPORTACAO.exists():
        logger.error(f"Pasta não encontrada: {PASTA_IMPORTACAO}")
        logger.info("Criando pasta...")
        PASTA_IMPORTACAO.mkdir(parents=True, exist_ok=True)
        logger.info("Pasta criada. Por favor, adicione os arquivos CSV e execute novamente.")
        return
    
    # Buscar arquivos CSV e XLSX de objetos
    arquivos_csv = list(PASTA_IMPORTACAO.glob("*.csv"))
    arquivos_objetos = list(PASTA_IMPORTACAO.glob("Relatorio_Objetos*.xlsx"))
    
    if not arquivos_csv:
        logger.warning("Nenhum arquivo CSV encontrado na pasta de importação.")
        return
    
    logger.info(f"Encontrados {len(arquivos_csv)} arquivo(s) CSV:")
    for arquivo in arquivos_csv:
        logger.info(f"  - {arquivo.name}")
    
    # Carregar e sincronizar Relatório de Objetos (logística) se existir
    objects_loader = None
    arquivo_objetos = None
    if arquivos_objetos:
        # Usar o mais recente
        arquivo_objetos = max(arquivos_objetos, key=lambda x: x.stat().st_mtime)
        logger.info(f"\nRelatório de Objetos encontrado: {arquivo_objetos.name}")
        objects_loader = ObjectsLoader(str(arquivo_objetos))
        logger.info(f"  Registros de logística carregados: {objects_loader.total_records}")
        
        # Sincronizar com o banco de dados
        logger.info("  Sincronizando Relatório de Objetos com banco de dados...")
        stats = db_manager.sync_relatorio_objetos(objects_loader)
        logger.info(f"  >> {stats['inseridos']} inseridos, {stats['atualizados']} atualizados, {stats['erros']} erros")
    else:
        logger.warning("\nNenhum Relatório de Objetos encontrado. Dados de logística não serão enriquecidos.")
    
    logger.info("")
    
    # Inicializar componentes
    db_path = "data/portabilidade.db"
    db_manager = DatabaseManager(db_path)
    
    # Criar pasta de saída WPP se necessário
    WPP_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    engine = QiggerDecisionEngine(
        db_manager, 
        triggers_path=str(TRIGGERS_PATH),
        objects_loader=objects_loader,
        wpp_output_path=str(WPP_OUTPUT_PATH)
    )
    
    # Exibir estatísticas das regras
    stats = engine.get_rules_stats()
    logger.info(f"Regras carregadas: {stats['total_regras']}")
    logger.info("")
    
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
            
            # Estatísticas de mapeamento
            mapeados = sum(1 for r in records if r.mapeado)
            nao_mapeados = len(records) - mapeados
            com_logistica = sum(1 for r in records if r.nome_cliente)
            com_template = sum(1 for r in records if r.template)
            
            logger.info(f"  Registros mapeados: {mapeados}")
            logger.info(f"  Registros não mapeados: {nao_mapeados}")
            logger.info(f"  Registros com dados de logística: {com_logistica}")
            logger.info(f"  Registros com Template (para WPP): {com_template}")
            
            if nao_mapeados > 0:
                logger.warning(f"⚠ {nao_mapeados} registro(s) não mapeado(s) foram adicionados ao triggers.xlsx para revisão")
            
            # Gerar saída WPP para registros com Template
            if com_template > 0:
                wpp_file = engine.generate_wpp_output(records)
                if wpp_file:
                    logger.info(f"✓ Arquivo WPP gerado: {wpp_file}")
            
            # Gerenciar saída - SEMPRE gerar arquivos de retorno
            success = erros_arquivo == 0
            result = output_manager.process_and_cleanup(
                arquivo_csv,
                success=success,
                records=records,
                results_map=results_map,
                objects_loader=objects_loader
            )
            
            if result['copied_to']:
                logger.info(f"✓ Planilhas geradas/copiadas para {len(result['copied_to'])} destino(s):")
                for path in result['copied_to']:
                    logger.info(f"  → {path}")
            else:
                logger.warning("⚠ Nenhum arquivo de retorno foi gerado.")
            
            # Excluir arquivos após processamento bem-sucedido
            arquivos_excluidos = []
            
            # Deletar CSV se processamento foi bem-sucedido
            if success and registros_processados > 0:
                try:
                    arquivo_csv.unlink()
                    arquivos_excluidos.append(arquivo_csv.name)
                    logger.info(f"✓ Arquivo CSV excluído após processamento: {arquivo_csv.name}")
                except Exception as e:
                    logger.warning(f"⚠ Não foi possível excluir arquivo CSV {arquivo_csv.name}: {e}")
            
            # Deletar Relatório de Objetos após sincronização (apenas uma vez, no último arquivo)
            if arquivo_objetos and arquivo_objetos.exists() and idx == total_arquivos:
                try:
                    arquivo_objetos.unlink()
                    arquivos_excluidos.append(arquivo_objetos.name)
                    logger.info(f"✓ Relatório de Objetos excluído após sincronização: {arquivo_objetos.name}")
                except Exception as e:
                    logger.warning(f"⚠ Não foi possível excluir Relatório de Objetos {arquivo_objetos.name}: {e}")
            
            if not arquivos_excluidos and success:
                logger.info("✓ Processamento concluído. Arquivos mantidos para verificação.")
            
            # Informar caminhos importantes
            db_abs_path = str(Path(db_path).absolute())
            logger.info("")
            logger.info("=" * 70)
            logger.info("INFORMAÇÕES IMPORTANTES:")
            logger.info("=" * 70)
            logger.info(f"Banco de dados: {db_abs_path}")
            logger.info(f"Arquivo triggers: {TRIGGERS_PATH}")
            logger.info(f"Saída WPP: {WPP_OUTPUT_PATH}")
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

