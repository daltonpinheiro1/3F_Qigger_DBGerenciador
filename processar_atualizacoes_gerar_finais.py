"""
Script unificado para:
1. Processar arquivos CSV de atualização da pasta data/entrada/
2. Gerar arquivos finais:
   - WPP (WhatsApp)
   - Aprovisionamento
   - Importação
"""
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Configurar encoding UTF-8
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
        logging.FileHandler('logs/processar_atualizacoes_gerar_finais.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.utils import CSVParser, ObjectsLoader
from src.utils.file_output_manager import FileOutputManager

# Caminhos de configuração (usar config centralizado)
try:
    from config import (
        DB_PATH, TRIGGERS_PATH, PASTA_IMPORTACOES, PASTA_ENTRADA,
        PASTA_PROCESSADOS, OUTPUT_WPP, OUTPUT_APROVISIONAMENTOS, OUTPUT_IMPORTACAO
    )
    TRIGGERS_PATH = Path(TRIGGERS_PATH)
    PASTA_ENTRADA = Path(PASTA_ENTRADA)
    PASTA_IMPORTACOES_ABSOLUTA = Path(PASTA_IMPORTACOES)
    OUTPUT_WPP = Path(OUTPUT_WPP)
    OUTPUT_APROVISIONAMENTOS = Path(OUTPUT_APROVISIONAMENTOS)
    OUTPUT_IMPORTACAO = Path(OUTPUT_IMPORTACAO)
    DB_PATH = DB_PATH  # Já é string do config
except ImportError:
    # Fallback se config.py não existir
    DB_PATH_RELATIVO = Path(__file__).parent / "data" / "portabilidade.db"
    DB_PATH = str(DB_PATH_RELATIVO)
    
    TRIGGERS_PATH = Path(__file__).parent / "triggers.xlsx"
    PASTA_IMPORTACOES_ABSOLUTA = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    PASTA_ENTRADA = Path(__file__).parent / "data" / "entrada"
    PASTA_PROCESSADOS = Path(__file__).parent / "data" / "processados"
    OUTPUT_WPP = Path(__file__).parent / "data" / "homologacao_wpp.csv"
    OUTPUT_APROVISIONAMENTOS = Path(__file__).parent / "data" / "homologacao_aprovisionamentos.csv"
    OUTPUT_IMPORTACAO = Path(__file__).parent / "data" / "importacao_final.csv"

# Garantir que PASTA_IMPORTACOES_ABSOLUTA sempre existe
if 'PASTA_IMPORTACOES_ABSOLUTA' not in locals():
    PASTA_IMPORTACOES_ABSOLUTA = Path("/Applications/Documentos/IMPORTACOES_QIGGER")

# Criar pastas se não existirem
PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
OUTPUT_WPP.parent.mkdir(parents=True, exist_ok=True)


def processar_arquivos_atualizacao() -> Dict[str, Any]:
    """
    Processa todos os arquivos CSV da pasta de entrada
    
    Returns:
        Estatísticas do processamento
    """
    stats = {
        'arquivos_processados': 0,
        'registros_processados': 0,
        'erros': 0,
        'arquivos': []
    }
    
    # Buscar arquivos CSV (na pasta de importações e/ou entrada)
    arquivos_csv = []
    
    # Buscar na pasta de importações absoluta
    if PASTA_IMPORTACOES_ABSOLUTA.exists():
        arquivos_importacoes = list(PASTA_IMPORTACOES_ABSOLUTA.glob("*.csv"))
        arquivos_csv.extend(arquivos_importacoes)
        logger.info(f"Encontrados {len(arquivos_importacoes)} arquivo(s) CSV em {PASTA_IMPORTACOES_ABSOLUTA}")
    
    # Buscar também na pasta relativa (se diferente)
    if PASTA_ENTRADA.exists() and PASTA_ENTRADA != PASTA_IMPORTACOES_ABSOLUTA:
        arquivos_relativos = list(PASTA_ENTRADA.glob("*.csv"))
        arquivos_csv.extend(arquivos_relativos)
        if arquivos_relativos:
            logger.info(f"Encontrados {len(arquivos_relativos)} arquivo(s) CSV em {PASTA_ENTRADA}")
    
    # Remover duplicatas
    arquivos_csv = list(set(arquivos_csv))
    
    if not arquivos_csv:
        logger.warning(f"Nenhum arquivo CSV encontrado em {PASTA_IMPORTACOES_ABSOLUTA} ou {PASTA_ENTRADA}")
        return stats
    
    logger.info(f"Total de {len(arquivos_csv)} arquivo(s) CSV únicos para processar")
    
    # Inicializar componentes
    db_manager = DatabaseManager(DB_PATH)
    
    # Buscar Relatório de Objetos se existir (na pasta de importações)
    objects_loader = None
    pasta_objetos = PASTA_IMPORTACOES_ABSOLUTA if PASTA_IMPORTACOES_ABSOLUTA.exists() else PASTA_ENTRADA
    arquivos_objetos = list(pasta_objetos.glob("Relatorio_Objetos*.xlsx"))
    if not arquivos_objetos and PASTA_ENTRADA != pasta_objetos:
        # Tentar também na pasta relativa
        arquivos_objetos = list(PASTA_ENTRADA.glob("Relatorio_Objetos*.xlsx"))
    
    if arquivos_objetos:
        arquivo_objetos = max(arquivos_objetos, key=lambda x: x.stat().st_mtime)
        try:
            objects_loader = ObjectsLoader(str(arquivo_objetos))
            logger.info(f"Relatório de Objetos carregado: {arquivo_objetos.name} ({objects_loader.total_records} registros)")
        except Exception as e:
            logger.warning(f"Erro ao carregar Relatório de Objetos: {e}")
    else:
        logger.info("Relatório de Objetos não encontrado (opcional)")
    
    engine = QiggerDecisionEngine(
        db_manager,
        triggers_path=str(TRIGGERS_PATH),
        objects_loader=objects_loader
    )
    
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
    for arquivo_csv in arquivos_csv:
        logger.info("=" * 70)
        logger.info(f"Processando: {arquivo_csv.name}")
        logger.info("=" * 70)
        
        try:
            # Parse do CSV
            records = CSVParser.parse_file(str(arquivo_csv))
            logger.info(f"Total de registros parseados: {len(records)}")
            
            if not records:
                logger.warning("Nenhum registro válido encontrado.")
                # Mover mesmo assim ao final do processo
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    nome_com_timestamp = f"{arquivo_csv.stem}_{timestamp}{arquivo_csv.suffix}"
                    if str(arquivo_csv.parent) == str(PASTA_IMPORTACOES_ABSOLUTA):
                        pasta_processados_importacoes = PASTA_IMPORTACOES_ABSOLUTA / "processados"
                        pasta_processados_importacoes.mkdir(parents=True, exist_ok=True)
                        destino = pasta_processados_importacoes / nome_com_timestamp
                    else:
                        PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
                        destino = PASTA_PROCESSADOS / nome_com_timestamp
                    arquivo_csv.rename(destino)
                    logger.info(f"✓ Arquivo movido para processados (sem registros): {destino.name}")
                except Exception as ex:
                    logger.warning(f"⚠ Não foi possível mover arquivo: {ex}")
                continue
            
            # Processar registros
            results_map = {}
            registros_processados = 0
            erros_arquivo = 0
            
            for i, record in enumerate(records, 1):
                try:
                    results = engine.process_record(record)
                    key = f"{record.cpf}_{record.numero_ordem}"
                    results_map[key] = results
                    registros_processados += 1
                    
                    if i % 100 == 0:
                        logger.info(f"  Progresso: {i}/{len(records)} registros processados...")
                        
                except Exception as e:
                    logger.error(f"Erro ao processar registro {i}: {e}")
                    erros_arquivo += 1
            
            logger.info(f"Arquivo processado: {registros_processados} registros, {erros_arquivo} erros")
            
            # Gerar arquivos de retorno
            success = erros_arquivo == 0
            result = output_manager.process_and_cleanup(
                arquivo_csv,
                success=success,
                records=records,
                results_map=results_map,
                objects_loader=objects_loader
            )
            
            if result['copied_to']:
                logger.info(f"✓ Planilhas geradas/copiadas para {len(result['copied_to'])} destino(s)")
            
            # Sempre mover arquivo para processados ao final do processo (com timestamp)
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_com_timestamp = f"{arquivo_csv.stem}_{timestamp}{arquivo_csv.suffix}"
                if str(arquivo_csv.parent) == str(PASTA_IMPORTACOES_ABSOLUTA):
                    pasta_processados_importacoes = PASTA_IMPORTACOES_ABSOLUTA / "processados"
                    pasta_processados_importacoes.mkdir(parents=True, exist_ok=True)
                    destino = pasta_processados_importacoes / nome_com_timestamp
                else:
                    PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
                    destino = PASTA_PROCESSADOS / nome_com_timestamp
                if arquivo_csv.exists():
                    arquivo_csv.rename(destino)
                    logger.info(f"✓ Arquivo movido para: {destino}")
            except Exception as e:
                logger.warning(f"⚠ Não foi possível mover arquivo para processados: {e}")
            
            stats['arquivos_processados'] += 1
            stats['registros_processados'] += registros_processados
            stats['arquivos'].append(arquivo_csv.name)
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo_csv.name}: {e}", exc_info=True)
            stats['erros'] += 1
            # Mover mesmo em caso de erro para não reprocessar
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_com_timestamp = f"{arquivo_csv.stem}_{timestamp}_erro{arquivo_csv.suffix}"
                if arquivo_csv.exists():
                    if str(arquivo_csv.parent) == str(PASTA_IMPORTACOES_ABSOLUTA):
                        pasta_processados_importacoes = PASTA_IMPORTACOES_ABSOLUTA / "processados"
                        pasta_processados_importacoes.mkdir(parents=True, exist_ok=True)
                        destino = pasta_processados_importacoes / nome_com_timestamp
                    else:
                        PASTA_PROCESSADOS.mkdir(parents=True, exist_ok=True)
                        destino = PASTA_PROCESSADOS / nome_com_timestamp
                    arquivo_csv.rename(destino)
                    logger.info(f"✓ Arquivo movido para processados (após erro): {destino.name}")
            except Exception as ex:
                logger.warning(f"⚠ Não foi possível mover arquivo após erro: {ex}")
    
    return stats


def gerar_arquivo_wpp() -> bool:
    """
    Gera arquivo final de WPP (WhatsApp)
    
    Returns:
        True se gerado com sucesso
    """
    try:
        logger.info("=" * 70)
        logger.info("Gerando arquivo final de WPP (WhatsApp)")
        logger.info("=" * 70)
        
        # Executar script de geração de WPP usando subprocess
        import subprocess
        import sys
        
        result = subprocess.run(
            [sys.executable, "gerar_homologacao_wpp.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.returncode == 0:
            if OUTPUT_WPP.exists():
                logger.info(f"✓ Arquivo WPP gerado: {OUTPUT_WPP}")
                return True
            else:
                logger.warning("⚠ Arquivo WPP não foi gerado")
                return False
        else:
            logger.error(f"Erro ao executar script WPP: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao gerar arquivo WPP: {e}", exc_info=True)
        return False


def gerar_arquivo_aprovisionamentos() -> bool:
    """
    Gera arquivo final de Aprovisionamentos
    
    Returns:
        True se gerado com sucesso
    """
    try:
        logger.info("=" * 70)
        logger.info("Gerando arquivo final de Aprovisionamentos")
        logger.info("=" * 70)
        
        # Executar script de geração de aprovisionamentos usando subprocess
        import subprocess
        import sys
        
        result = subprocess.run(
            [sys.executable, "gerar_homologacao_aprovisionamentos.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )
        
        if result.returncode == 0:
            if OUTPUT_APROVISIONAMENTOS.exists():
                logger.info(f"✓ Arquivo de Aprovisionamentos gerado: {OUTPUT_APROVISIONAMENTOS}")
                return True
            else:
                logger.warning("⚠ Arquivo de Aprovisionamentos não foi gerado")
                return False
        else:
            logger.error(f"Erro ao executar script Aprovisionamentos: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao gerar arquivo de Aprovisionamentos: {e}", exc_info=True)
        return False


def gerar_arquivo_importacao() -> bool:
    """
    Gera arquivo final de Importação (consolida todos os retornos)
    
    Returns:
        True se gerado com sucesso
    """
    try:
        logger.info("=" * 70)
        logger.info("Gerando arquivo final de Importação")
        logger.info("=" * 70)
        
        db_manager = DatabaseManager(DB_PATH)
        
        # Buscar todos os registros processados recentemente
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    cpf, numero_acesso, numero_ordem, codigo_externo,
                    status_bilhete, status_ordem, operadora_doadora,
                    data_portabilidade, motivo_recusa, motivo_cancelamento,
                    regra_id, o_que_aconteceu, acao_a_realizar,
                    tipo_mensagem, template, mapeado,
                    created_at, updated_at
                FROM portabilidade_records
                WHERE updated_at >= datetime('now', '-1 day')
                ORDER BY updated_at DESC
            """)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        
        if not rows:
            logger.warning("Nenhum registro recente encontrado para importação")
            return False
        
        # Gerar CSV de importação
        import csv
        
        OUTPUT_IMPORTACAO.parent.mkdir(parents=True, exist_ok=True)
        
        with open(OUTPUT_IMPORTACAO, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=columns, delimiter=';')
            writer.writeheader()
            
            for row in rows:
                writer.writerow(dict(zip(columns, row)))
        
        logger.info(f"✓ Arquivo de Importação gerado: {OUTPUT_IMPORTACAO}")
        logger.info(f"  Total de registros: {len(rows)}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao gerar arquivo de Importação: {e}", exc_info=True)
        return False


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Processar atualizações e gerar arquivos finais (WPP, Aprovisionamento, Importação)"
    )
    
    parser.add_argument(
        '--apenas-processar',
        action='store_true',
        help='Apenas processar arquivos de atualização, sem gerar arquivos finais'
    )
    
    parser.add_argument(
        '--apenas-gerar',
        action='store_true',
        help='Apenas gerar arquivos finais, sem processar atualizações'
    )
    
    parser.add_argument(
        '--wpp',
        action='store_true',
        help='Gerar apenas arquivo WPP'
    )
    
    parser.add_argument(
        '--aprovisionamentos',
        action='store_true',
        help='Gerar apenas arquivo de Aprovisionamentos'
    )
    
    parser.add_argument(
        '--importacao',
        action='store_true',
        help='Gerar apenas arquivo de Importação'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("PROCESSADOR DE ATUALIZAÇÕES E GERADOR DE ARQUIVOS FINAIS")
    logger.info("=" * 70)
    logger.info(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("")
    logger.info(f"Pasta de importações: {PASTA_IMPORTACOES_ABSOLUTA if PASTA_IMPORTACOES_ABSOLUTA.exists() else 'Não encontrada'}")
    logger.info(f"Pasta de entrada: {PASTA_ENTRADA}")
    logger.info(f"Banco de dados: {DB_PATH}")
    logger.info("")
    
    # Processar arquivos de atualização
    if not args.apenas_gerar:
        logger.info("ETAPA 1: Processando arquivos de atualização...")
        logger.info("")
        stats = processar_arquivos_atualizacao()
        
        logger.info("")
        logger.info("Resumo do processamento:")
        logger.info(f"  Arquivos processados: {stats['arquivos_processados']}")
        logger.info(f"  Registros processados: {stats['registros_processados']}")
        logger.info(f"  Erros: {stats['erros']}")
        logger.info("")
    
    # Gerar arquivos finais
    if not args.apenas_processar:
        logger.info("ETAPA 2: Gerando arquivos finais...")
        logger.info("")
        
        resultados = {
            'wpp': False,
            'aprovisionamentos': False,
            'importacao': False
        }
        
        # Gerar WPP
        if args.wpp or (not args.wpp and not args.aprovisionamentos and not args.importacao):
            resultados['wpp'] = gerar_arquivo_wpp()
            logger.info("")
        
        # Gerar Aprovisionamentos
        if args.aprovisionamentos or (not args.wpp and not args.aprovisionamentos and not args.importacao):
            resultados['aprovisionamentos'] = gerar_arquivo_aprovisionamentos()
            logger.info("")
        
        # Gerar Importação
        if args.importacao or (not args.wpp and not args.aprovisionamentos and not args.importacao):
            resultados['importacao'] = gerar_arquivo_importacao()
            logger.info("")
        
        # Resumo
        logger.info("=" * 70)
        logger.info("RESUMO DA GERAÇÃO DE ARQUIVOS FINAIS")
        logger.info("=" * 70)
        logger.info(f"  WPP (WhatsApp): {'✓' if resultados['wpp'] else '✗'}")
        logger.info(f"  Aprovisionamentos: {'✓' if resultados['aprovisionamentos'] else '✗'}")
        logger.info(f"  Importação: {'✓' if resultados['importacao'] else '✗'}")
        logger.info("")
        logger.info("Arquivos gerados em:")
        logger.info(f"  - WPP: {OUTPUT_WPP}")
        logger.info(f"  - Aprovisionamentos: {OUTPUT_APROVISIONAMENTOS}")
        logger.info(f"  - Importação: {OUTPUT_IMPORTACAO}")
        logger.info("=" * 70)
    
    logger.info("")
    logger.info("Processamento concluído!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

