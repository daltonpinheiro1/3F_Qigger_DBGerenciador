"""
Processa arquivos Excel de ESTORNOS (Estornadas.xlsx).
Cria a tabela estornos no portabilidade.db.
"""
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

TABELA = "estornos"


def processar_estornos(arquivo_path: str, db_path: str) -> Dict[str, Any]:
    """
    Processa Excel de Estornadas.
    Cria tabela estornos com colunas: gsm (numero_acesso), origem_arquivo, data_importacao.

    Returns:
        Estatísticas: total_linhas, inseridos, erros
    """
    stats = {'total_linhas': 0, 'inseridos': 0, 'erros': 0}

    path = Path(arquivo_path)
    if not path.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_path}")
        return stats

    try:
        import pandas as pd
        df = pd.read_excel(path, engine='openpyxl')
    except Exception as e:
        logger.error(f"Erro ao ler Excel {path.name}: {e}")
        return stats

    if df.empty:
        logger.warning(f"Arquivo vazio: {path.name}")
        return stats

    # Normalizar colunas: gsm, GSM, ou primeira coluna numérica
    col_gsm = None
    for c in df.columns:
        c_str = str(c).strip().lower()
        if c_str == 'gsm' or c_str == 'numero_acesso' or c_str == 'numero de acesso' or c_str == 'número de acesso':
            col_gsm = c
            break
    if col_gsm is None:
        col_gsm = df.columns[0]

    # Extrair valores de gsm (limpar e deduplicar)
    valores = df[col_gsm].dropna().astype(str).str.strip().str.rstrip(',')
    valores = valores[valores.str.match(r'^\d{10,15}$', na=False)]
    valores = valores.unique().tolist()

    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABELA} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gsm TEXT NOT NULL,
                numero_acesso TEXT,
                origem_arquivo TEXT,
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_estornos_gsm ON {TABELA}(gsm)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_estornos_numero_acesso ON {TABELA}(numero_acesso)")

        for gsm in valores:
            try:
                num = gsm.lstrip('0') if gsm else ''
                cursor.execute(
                    f"INSERT INTO {TABELA} (gsm, numero_acesso, origem_arquivo) VALUES (?, ?, ?)",
                    (gsm, num or gsm, path.name)
                )
                stats['inseridos'] += 1
            except Exception as e:
                logger.warning(f"Erro ao inserir gsm {gsm}: {e}")
                stats['erros'] += 1

        stats['total_linhas'] = len(valores)
        conn.commit()
        logger.info(f"{TABELA}: {stats['inseridos']} registros de {path.name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao processar Estornos: {e}")
        raise
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    import sys
    from config import DB_PATH, PASTA_IMPORTACOES

    pasta = Path(PASTA_IMPORTACOES)
    db = DB_PATH

    arquivos = list(pasta.glob("*Estornad*.xlsx")) + list(pasta.glob("*estorno*.xlsx"))
    if not arquivos:
        arquivos = [f for f in pasta.glob("*.xlsx") if "estorno" in f.name.lower()]

    if sys.argv[1:]:
        arquivos = [Path(p) for p in sys.argv[1:]]

    for arq in arquivos:
        if arq.exists():
            st = processar_estornos(str(arq), db)
            print(f"  {arq.name}: {st['inseridos']} inseridos, {st['erros']} erros")
