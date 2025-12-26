"""
Processar Régua de Comunicação WhatsApp
Versão 1.0 - Baseado na base_analitica_final.csv
"""
import sys
import logging
from pathlib import Path
from datetime import datetime

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
        logging.FileHandler('logs/regua_comunicacao.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.utils import ReguaComunicacao

# Caminhos de configuração
BASE_ANALITICA_PATH = Path(r"G:\Meu Drive\3F Contact Center\base_analitica_final.csv")
WPP_OUTPUT_PATH = Path(r"G:\Meu Drive\3F Contact Center\WPP_Regua_Output.csv")


def processar_regua():
    """Processa a base analítica e gera a régua de comunicação"""
    
    logger.info("=" * 70)
    logger.info("3F Qigger - Régua de Comunicação WhatsApp")
    logger.info("Versão 1.0 - Processamento da Base Analítica")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Base analítica: {BASE_ANALITICA_PATH}")
    logger.info(f"Saída WPP: {WPP_OUTPUT_PATH}")
    logger.info("")
    
    # Verificar se base existe
    if not BASE_ANALITICA_PATH.exists():
        logger.error(f"Base analítica não encontrada: {BASE_ANALITICA_PATH}")
        return
    
    # Inicializar régua
    regua = ReguaComunicacao(str(BASE_ANALITICA_PATH))
    
    logger.info("=" * 70)
    logger.info("PROCESSANDO BASE ANALÍTICA")
    logger.info("=" * 70)
    
    # Processar apenas vendas aprovadas
    disparos = regua.processar_base(filtros={'Status venda': 'APROVADA'})
    
    if not disparos:
        logger.warning("Nenhum disparo identificado")
        return
    
    # Estatísticas
    stats = regua.get_estatisticas()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("ESTATÍSTICAS DE DISPAROS")
    logger.info("=" * 70)
    logger.info(f"Total de registros na base: {stats['base_registros']}")
    logger.info(f"Total de disparos identificados: {stats['total']}")
    logger.info("")
    logger.info("Por Tipo de Comunicação:")
    for tipo, count in stats['por_tipo'].items():
        logger.info(f"  {tipo}: {count}")
    
    # Gerar CSV de saída
    logger.info("")
    logger.info("=" * 70)
    logger.info("GERANDO ARQUIVO DE SAÍDA")
    logger.info("=" * 70)
    
    # Criar pasta se não existir
    WPP_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Gerar arquivo (sobrescrever)
    output_file = regua.gerar_csv_disparos(str(WPP_OUTPUT_PATH), append=False)
    
    if output_file:
        logger.info(f"✓ Arquivo gerado com sucesso: {output_file}")
    else:
        logger.error("✗ Falha ao gerar arquivo de saída")
    
    # Resumo final
    logger.info("")
    logger.info("=" * 70)
    logger.info("PROCESSAMENTO CONCLUÍDO")
    logger.info("=" * 70)
    logger.info(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info(f"Disparos gerados: {stats['total']}")
    logger.info(f"Arquivo: {WPP_OUTPUT_PATH}")
    logger.info("=" * 70)


def main():
    """Função principal com argumentos de linha de comando"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="3F Qigger - Régua de Comunicação WhatsApp"
    )
    
    parser.add_argument(
        '--base',
        type=str,
        default=str(BASE_ANALITICA_PATH),
        help=f'Caminho para base_analitica_final.csv (padrão: {BASE_ANALITICA_PATH})'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=str(WPP_OUTPUT_PATH),
        help=f'Caminho para arquivo de saída WPP (padrão: {WPP_OUTPUT_PATH})'
    )
    
    parser.add_argument(
        '--append',
        action='store_true',
        help='Adicionar ao arquivo existente ao invés de sobrescrever'
    )
    
    parser.add_argument(
        '--status',
        type=str,
        default='APROVADA',
        help='Filtrar por Status venda (padrão: APROVADA)'
    )
    
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Apenas exibir estatísticas, sem gerar arquivo'
    )
    
    args = parser.parse_args()
    
    # Usar caminhos dos argumentos
    base_path = Path(args.base)
    output_path = Path(args.output)
    
    logger.info("=" * 70)
    logger.info("3F Qigger - Régua de Comunicação WhatsApp")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Base analítica: {base_path}")
    logger.info(f"Saída WPP: {output_path}")
    logger.info(f"Filtro Status: {args.status}")
    logger.info("")
    
    # Verificar se base existe
    if not base_path.exists():
        logger.error(f"Base analítica não encontrada: {base_path}")
        sys.exit(1)
    
    # Inicializar régua
    regua = ReguaComunicacao(str(base_path))
    
    # Processar
    filtros = {'Status venda': args.status} if args.status else None
    disparos = regua.processar_base(filtros=filtros)
    
    # Estatísticas
    stats = regua.get_estatisticas()
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("ESTATÍSTICAS")
    logger.info("=" * 70)
    logger.info(f"Registros na base: {stats['base_registros']}")
    logger.info(f"Disparos identificados: {stats['total']}")
    logger.info("")
    
    for tipo, count in stats['por_tipo'].items():
        pct = (count / stats['total'] * 100) if stats['total'] > 0 else 0
        logger.info(f"  {tipo}: {count} ({pct:.1f}%)")
    
    # Gerar arquivo se não for apenas stats
    if not args.stats_only and disparos:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_file = regua.gerar_csv_disparos(str(output_path), append=args.append)
        if output_file:
            logger.info(f"\n✓ Arquivo gerado: {output_file}")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            main()
        else:
            processar_regua()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
