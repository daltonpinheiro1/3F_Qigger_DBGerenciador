"""
Processa o arquivo BS_VENDA_DU.xlsx do SMB (02 Planejamento/08 - Relatorios Cliente)
e atualiza a tabela bs_venda_du no portabilidade.db.

Coleta e atualização: a cada execução a tabela é recarregada com o conteúdo do Excel.
"""
import re
import logging
import sys
from pathlib import Path
from datetime import datetime

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/processar_bs_venda_du.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

try:
    from config import DB_PATH, ARQUIVO_BS_VENDA_DU_NETWORK, PASTA_BS_VENDA_DU_LOCAL, PASTA_ENTRADA
except ImportError:
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")
    ARQUIVO_BS_VENDA_DU_NETWORK = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/BS_VENDA_DU.xlsx")
    PASTA_BS_VENDA_DU_LOCAL = Path(__file__).parent / "data" / "entrada" / "excel"
    PASTA_ENTRADA = Path(__file__).parent / "data" / "entrada"

TABELA = "bs_venda_du"
SMB_PATH_REL = "02 - Relatórios/08 - Relatorios Cliente"
SMB_MOUNT = Path("/Volumes/02 Planejamento")


def _sanitizar_coluna(nome: str) -> str:
    """Nome de coluna para identificador SQL válido."""
    s = str(nome).strip()
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s[:64] if len(s) > 64 else s) or "col_0"


def _encontrar_arquivo() -> Path:
    """Localiza BS_VENDA_DU.xlsx: SMB > rede > local > entrada."""
    # 1) Rede (SMB montado)
    if ARQUIVO_BS_VENDA_DU_NETWORK and Path(ARQUIVO_BS_VENDA_DU_NETWORK).exists():
        return Path(ARQUIVO_BS_VENDA_DU_NETWORK)
    # 2) Pasta SMB pelo mount
    pasta_smb = SMB_MOUNT / SMB_PATH_REL
    if pasta_smb.exists():
        for f in pasta_smb.glob("BS_VENDA_DU*.xlsx"):
            return f
    # 3) Local
    for base in (PASTA_BS_VENDA_DU_LOCAL, PASTA_ENTRADA):
        if base.exists():
            for f in base.glob("BS_VENDA_DU*.xlsx"):
                return f
    return None


def processar_bs_venda_du(arquivo: Path = None, db_path: str = None) -> dict:
    """
    Lê o Excel BS_VENDA_DU e atualiza a tabela bs_venda_du (replace completo).
    """
    import sqlite3
    import pandas as pd

    db_path = db_path or str(DB_PATH)
    arquivo = arquivo or _encontrar_arquivo()
    stats = {"sucesso": False, "linhas": 0, "colunas": 0, "erros": 0}

    if not arquivo or not arquivo.exists():
        logger.warning("BS_VENDA_DU.xlsx não encontrado (SMB/rede/local/entrada)")
        return stats

    try:
        df = pd.read_excel(arquivo, engine="openpyxl", dtype=str)
    except Exception as e:
        logger.error(f"Erro ao ler Excel: {e}")
        stats["erros"] += 1
        return stats

    if df.empty:
        logger.warning("Excel sem linhas")
        return stats

    # Colunas SQL a partir do cabeçalho
    df.columns = [str(c).strip() for c in df.columns]
    colunas_sql = [_sanitizar_coluna(c) for c in df.columns]
    # Garantir nomes únicos
    seen = {}
    for i, c in enumerate(colunas_sql):
        if not c:
            colunas_sql[i] = f"col_{i}"
        elif c in seen:
            seen[c] += 1
            colunas_sql[i] = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
    df.columns = colunas_sql
    stats["colunas"] = len(colunas_sql)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Criar tabela se não existir (todas as colunas TEXT)
        cols_ddl = ["id INTEGER PRIMARY KEY AUTOINCREMENT", "data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP"]
        cols_ddl += [f'"{c}" TEXT' for c in colunas_sql]
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELA} ({', '.join(cols_ddl)})")

        # Se a tabela já existir, adicionar colunas novas
        cur.execute(f"PRAGMA table_info({TABELA})")
        existentes = {row[1] for row in cur.fetchall()}
        for c in colunas_sql:
            if c not in existentes and c not in ("id", "data_importacao"):
                try:
                    cur.execute(f'ALTER TABLE {TABELA} ADD COLUMN "{c}" TEXT')
                    existentes.add(c)
                except sqlite3.OperationalError:
                    pass

        # Inserir apenas em colunas que existem na tabela
        cols_insert = [c for c in colunas_sql if c in existentes]
        if not cols_insert:
            cols_insert = [c for c in colunas_sql]
        cur.execute(f"DELETE FROM {TABELA}")
        placeholders = ", ".join(["?" for _ in cols_insert])
        cols_quoted = ", ".join(f'"{c}"' for c in cols_insert)
        sql = f"INSERT INTO {TABELA} ({cols_quoted}) VALUES ({placeholders})"
        for _, row in df.iterrows():
            vals = [str(v) if pd.notna(v) else None for v in row[cols_insert]]
            try:
                cur.execute(sql, vals)
                stats["linhas"] += 1
            except Exception as e:
                stats["erros"] += 1
                logger.debug(f"Erro ao inserir linha: {e}")
        conn.commit()
        stats["sucesso"] = True
        logger.info(f"bs_venda_du: {stats['linhas']} registros de {arquivo.name}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao atualizar tabela: {e}")
        stats["erros"] += 1
    finally:
        conn.close()

    return stats


def main():
    import argparse
    p = argparse.ArgumentParser(description="Processar BS_VENDA_DU.xlsx → tabela bs_venda_du")
    p.add_argument("--db", default=str(DB_PATH), help="Caminho do banco")
    p.add_argument("--arquivo", type=Path, help="Caminho do Excel (opcional)")
    args = p.parse_args()
    stats = processar_bs_venda_du(arquivo=args.arquivo, db_path=args.db)
    print(f"Resultado: {stats['linhas']} linhas, {stats['colunas']} colunas, {stats['erros']} erros")
    return 0 if stats["sucesso"] else 1


if __name__ == "__main__":
    sys.exit(main())
