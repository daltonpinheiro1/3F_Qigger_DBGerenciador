"""
Processa auditoria de vendas TIM Pré/Controle.

Executa o pipeline completo: coleta EVA → retornos RPA → cruzamento.
Arquivos CSV de retorno RPA são detectados automaticamente em
PASTA_IMPORTACOES ou aceitos via argumentos de linha de comando.
"""

import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from config import (
    DB_V2_PATH,
    LOG_LEVEL,
    PASTA_IMPORTACOES,
    PASTA_PROCESSADOS,
)
from src.database.db_manager_v2 import DatabaseManagerV2
from src.pipeline_auditoria.pipeline import PipelineAuditoria

# Configurar logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _detectar_arquivos_rpa() -> list[str]:
    """Detecta arquivos CSV de retorno RPA TIM Pre/Ctrl em PASTA_IMPORTACOES."""
    pasta = Path(PASTA_IMPORTACOES)
    if not pasta.exists():
        logger.warning("Pasta de importações não encontrada: %s", pasta)
        return []
    # Buscar padrão TIM PRE CRTL Detalhado
    arquivos = sorted(
        str(f) for f in pasta.glob("*TIM*PRE*CRTL*Detalhado*.csv")
        if f.is_file()
    )
    if not arquivos:
        arquivos = sorted(
            str(f) for f in pasta.glob("*TIM*PRE*Detalhado*.csv")
            if f.is_file()
        )
    logger.info(
        "Detectados %d arquivos retorno RPA em %s", len(arquivos), pasta
    )
    return arquivos


def _mover_processados(arquivos: list[str]) -> int:
    """Move arquivos processados para PASTA_PROCESSADOS com prefixo timestamp.

    Returns:
        Quantidade de arquivos movidos com sucesso.
    """
    pasta_dest = Path(PASTA_PROCESSADOS)
    pasta_dest.mkdir(parents=True, exist_ok=True)
    movidos = 0

    for caminho in arquivos:
        origem = Path(caminho)
        if not origem.exists():
            continue
        prefixo = datetime.now().strftime("%Y%m%d%H%M%S")
        destino = pasta_dest / f"{prefixo}_{origem.name}"
        try:
            shutil.move(str(origem), str(destino))
            logger.info("Movido: %s → %s", origem.name, destino.name)
            movidos += 1
        except OSError:
            logger.exception("Erro ao mover %s", origem.name)

    return movidos


def main():
    """Ponto de entrada principal do pipeline de auditoria."""
    logger.info("=" * 60)
    logger.info("PIPELINE DE AUDITORIA DE VENDAS TIM PRÉ/CONTROLE")
    logger.info("=" * 60)

    # Determinar arquivos RPA
    if sys.argv[1:]:
        arquivos_rpa = sys.argv[1:]
        logger.info(
            "Recebidos %d arquivos via linha de comando",
            len(arquivos_rpa),
        )
    else:
        arquivos_rpa = _detectar_arquivos_rpa()

    if not arquivos_rpa:
        logger.warning("Nenhum arquivo CSV de retorno RPA encontrado.")

    # Instanciar componentes
    db_manager = DatabaseManagerV2(DB_V2_PATH)
    pipeline = PipelineAuditoria(db_manager)

    # Executar pipeline
    stats = pipeline.executar(arquivos_rpa if arquivos_rpa else None)

    # Mover arquivos processados
    if arquivos_rpa:
        movidos = _mover_processados(arquivos_rpa)
        logger.info("Arquivos movidos para processados: %d", movidos)

    # Resumo final
    duracao = stats.get("duracao_segundos", 0)
    cruzamento = stats.get("cruzamento", {})
    rpa_list = stats.get("rpa", [])
    total_rpa = sum(r.get("inseridos", 0) for r in rpa_list)

    print()
    print("=" * 60)
    print("RESUMO DA EXECUÇÃO")
    print("=" * 60)
    print(
        f"  EVA: {stats.get('eva', {}).get('inseridos', 0)} "
        f"registros coletados"
    )
    print(f"  RPA: {total_rpa} registros processados")
    print(
        f"  Cruzamento: {cruzamento.get('total', 0)} registros "
        f"({cruzamento.get('com_match', 0)} com match, "
        f"{cruzamento.get('eva_sem_rpa', 0)} EVA sem RPA, "
        f"{cruzamento.get('rpa_sem_eva', 0)} RPA sem EVA)"
    )
    print(f"  Duração: {duracao:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecução interrompida pelo usuário.")
        sys.exit(1)
    except Exception:
        logger.exception("Erro fatal no pipeline de auditoria")
        sys.exit(1)
