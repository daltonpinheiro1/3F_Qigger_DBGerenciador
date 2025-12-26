"""
Processar Régua de Comunicação WhatsApp - Versão DINÂMICA
Cruza múltiplas fontes para determinar status atual de cada proposta
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

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
        logging.FileHandler('logs/regua_dinamica.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.utils.regua_comunicacao_dinamica import ReguaComunicacaoDinamica

# Caminhos de configuração
BASE_ANALITICA_PATH = Path(r"G:\Meu Drive\3F Contact Center\base_analitica_final.csv")
PASTA_IMPORTACAO = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
WPP_OUTPUT_PATH = Path(r"G:\Meu Drive\3F Contact Center\WPP_Regua_Dinamica.csv")


def encontrar_arquivo_mais_recente(pasta: Path, pattern: str) -> Path:
    """Encontra o arquivo mais recente que corresponde ao padrão"""
    arquivos = list(pasta.glob(pattern))
    if not arquivos:
        return None
    return max(arquivos, key=lambda x: x.stat().st_mtime)


def processar_regua_dinamica():
    """Processa a régua de comunicação dinâmica"""
    
    logger.info("=" * 70)
    logger.info("3F Qigger - Régua de Comunicação DINÂMICA")
    logger.info("Cruzamento de múltiplas fontes de dados")
    logger.info("=" * 70)
    logger.info("")
    
    # Inicializar régua dinâmica
    regua = ReguaComunicacaoDinamica()
    
    # === CARREGAR FONTES DE DADOS ===
    
    logger.info("CARREGANDO FONTES DE DADOS:")
    logger.info("-" * 40)
    
    # 1. Base Analítica
    if BASE_ANALITICA_PATH.exists():
        count = regua.carregar_base_analitica(str(BASE_ANALITICA_PATH))
        logger.info(f"✓ Base Analítica: {count} propostas")
    else:
        logger.warning(f"✗ Base Analítica não encontrada: {BASE_ANALITICA_PATH}")
    
    # 2. Relatório de Objetos (logística)
    arquivo_objetos = encontrar_arquivo_mais_recente(PASTA_IMPORTACAO, "Relatorio_Objetos*.xlsx")
    if arquivo_objetos:
        count = regua.carregar_relatorio_objetos(str(arquivo_objetos))
        logger.info(f"✓ Relatório Objetos: {count} propostas ({arquivo_objetos.name})")
    else:
        logger.warning(f"✗ Relatório de Objetos não encontrado em: {PASTA_IMPORTACAO}")
    
    # 3. CSV Portabilidade (Siebel)
    arquivo_port = encontrar_arquivo_mais_recente(PASTA_IMPORTACAO, "*portabilidade*.csv")
    if not arquivo_port:
        arquivo_port = encontrar_arquivo_mais_recente(PASTA_IMPORTACAO, "*.csv")
    
    if arquivo_port:
        count = regua.carregar_csv_portabilidade(str(arquivo_port))
        logger.info(f"✓ CSV Portabilidade: {count} propostas ({arquivo_port.name})")
    else:
        logger.warning(f"✗ CSV Portabilidade não encontrado em: {PASTA_IMPORTACAO}")
    
    logger.info("")
    
    # === PROCESSAR PROPOSTAS ===
    
    logger.info("=" * 70)
    logger.info("PROCESSANDO PROPOSTAS")
    logger.info("=" * 70)
    
    disparos = regua.processar_todas_propostas()
    
    # === ESTATÍSTICAS ===
    
    stats = regua.get_estatisticas()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("ESTATÍSTICAS")
    logger.info("=" * 70)
    logger.info(f"Propostas únicas processadas: {stats['total_propostas']}")
    logger.info(f"Disparos identificados: {stats['total_disparos']}")
    logger.info("")
    
    logger.info("Por Fonte de Dados:")
    for fonte, count in stats['por_fonte'].items():
        logger.info(f"  {fonte}: {count}")
    
    logger.info("")
    logger.info("Por Tipo de Comunicação:")
    for tipo, count in stats['por_tipo'].items():
        pct = (count / stats['total_disparos'] * 100) if stats['total_disparos'] > 0 else 0
        logger.info(f"  {tipo}: {count} ({pct:.1f}%)")
    
    # === GERAR ARQUIVO DE SAÍDA ===
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("GERANDO ARQUIVO DE SAÍDA")
    logger.info("=" * 70)
    
    WPP_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    output_file = regua.gerar_csv_disparos(str(WPP_OUTPUT_PATH))
    
    if output_file:
        logger.info(f"✓ Arquivo gerado: {output_file}")
    else:
        logger.error("✗ Falha ao gerar arquivo")
    
    # Resumo final
    logger.info("")
    logger.info("=" * 70)
    logger.info("PROCESSAMENTO CONCLUÍDO")
    logger.info("=" * 70)
    logger.info(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info(f"Disparos gerados: {stats['total_disparos']}")
    logger.info(f"Arquivo: {WPP_OUTPUT_PATH}")
    logger.info("=" * 70)


if __name__ == "__main__":
    try:
        processar_regua_dinamica()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
