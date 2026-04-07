"""
Script para gerar arquivo de homologação de Consulta.
Lista vendas com confirmação de entrega (entregue / pedido entregue)
usando exclusivamente o banco V2 (portabilidade_v2.db).

Integrado ao processar_completo: gera data/homologacao_consulta.xlsx.
"""
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import pandas as pd
from src.utils.data_integrity import sanitizar_valor

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_consulta.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Caminhos
try:
    from config import DB_V2_PATH, PROJECT_ROOT
    OUTPUT_PATH = PROJECT_ROOT / "data" / "homologacao_consulta.xlsx"
except ImportError:
    DB_V2_PATH = str(Path(__file__).parent / "data" / "portabilidade_v2.db")
    OUTPUT_PATH = Path(__file__).parent / "data" / "homologacao_consulta.xlsx"

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Importar QueriesV2
try:
    from src.database.queries_v2 import QueriesV2
    _V2_AVAILABLE = bool(DB_V2_PATH) and Path(DB_V2_PATH).exists()
except (ImportError, Exception):
    _V2_AVAILABLE = False

# Respeitar flags propagadas via env vars
if os.environ.get('QIGGER_FORCAR_LEGADO') == '1':
    _V2_AVAILABLE = False
elif os.environ.get('QIGGER_FORCAR_V2') == '1' and DB_V2_PATH:
    _V2_AVAILABLE = True

DIAS_LIMITE = 180


def _limpar_telefone(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    s = str(val).strip()
    return ''.join(c for c in s if c.isdigit()) if s else ''


def _eh_valido(num: str) -> bool:
    n = _limpar_telefone(num)
    return 10 <= len(n) <= 15 if n else False


def gerar_homologacao_consulta() -> int:
    """Gera arquivo de homologação de consulta (vendas com entrega confirmada)."""
    print("=" * 70)
    print("GERAÇÃO DE HOMOLOGAÇÃO DE CONSULTA (V2)")
    print("=" * 70)
    print()
    print("Critérios:")
    print(f"  - Fonte: banco V2 (QueriesV2) — últimos {DIAS_LIMITE} dias")
    print("  - Filtro: vendas com confirmação de entrega (entregue / pedido entregue)")
    print("  - Portabilidade: 2 linhas (número portado + nova linha)")
    print("  - Aquisição: 1 linha (só nova linha)")
    print("  - Exclusão: rejeição SMS")
    print()

    if not _V2_AVAILABLE:
        print("    >> ❌ Banco V2 não disponível. Nada a gerar.")
        logger.error("Banco V2 não disponível: %s", DB_V2_PATH)
        return 1

    print("[1] Consultando banco V2...")
    queries_v2 = QueriesV2(DB_V2_PATH)
    registros = queries_v2.buscar_registros_consulta(dias_limite=DIAS_LIMITE)

    if not registros:
        print("    >> ⚠ Nenhuma venda com confirmação de entrega encontrada.")
        return 0

    print(f"    >> ✅ {len(registros)} registros obtidos via QueriesV2")

    # [2] Expandir linhas: Portabilidade = 2 linhas, Aquisição = 1 linha
    print("[2] Expandindo linhas (Portabilidade vs Aquisição)...")
    dados = []
    for r in registros:
        cpf = str(r.get('cpf') or '').strip()
        codigo = str(r.get('codigo_externo') or '').strip()
        numero_ordem = str(r.get('numero_ordem') or '').strip()
        num_portado = _limpar_telefone(r.get('telefone_portado'))
        nova_linha = _limpar_telefone(r.get('numero_linha'))
        num_acesso = _limpar_telefone(r.get('numero_acesso'))

        if _eh_valido(num_portado) and _eh_valido(nova_linha) and num_portado != nova_linha:
            dados.append({'Cpf': cpf, 'Número de acesso': num_portado,
                          'Número da ordem': numero_ordem, 'Código externo': codigo})
            dados.append({'Cpf': cpf, 'Número de acesso': nova_linha,
                          'Número da ordem': numero_ordem, 'Código externo': codigo})
        else:
            num_consulta = nova_linha or num_portado or num_acesso
            if num_consulta:
                dados.append({'Cpf': cpf, 'Número de acesso': num_consulta,
                              'Número da ordem': numero_ordem, 'Código externo': codigo})

    # [3] Deduplicação por (cpf, numero_acesso)
    print("[3] Deduplicando...")
    vistos = set()
    dados_dedup = []
    for d in dados:
        chave = (d.get('Cpf', '').strip(), d.get('Número de acesso', '').strip())
        if chave not in vistos:
            vistos.add(chave)
            dados_dedup.append(d)
    if len(dados) != len(dados_dedup):
        print(f"    >> Deduplicação: {len(dados)} → {len(dados_dedup)} registros")
    dados = dados_dedup

    # [4] Gerar arquivo
    print("[4] Gerando arquivo...")
    df = pd.DataFrame(dados)
    colunas = ['Cpf', 'Número de acesso', 'Número da ordem', 'Código externo']
    df = df[[c for c in colunas if c in df.columns]]

    for col in df.columns:
        df[col] = df[col].apply(lambda v: sanitizar_valor(v) if v is not None else '')

    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Consulta')
        ws = writer.sheets['Consulta']
        ws.column_dimensions['A'].width = 15
        ws.column_dimensions['B'].width = 20
        ws.column_dimensions['C'].width = 25
        ws.column_dimensions['D'].width = 15
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
            for cell in row:
                cell.number_format = '@'

    print(f"    >> Arquivo gerado: {OUTPUT_PATH}")
    print(f"    >> Total: {len(dados):,} linhas a consultar")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(gerar_homologacao_consulta())
    except KeyboardInterrupt:
        print("\nProcessamento interrompido.")
        sys.exit(1)
    except Exception as e:
        logger.error("Erro fatal: %s", e, exc_info=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
