"""
Processa arquivos CSV de TIM PRE - CONTROLE (Migrar plano Pre x Controle).
Cria a tabela tim_pre_controle no portabilidade.db.
"""
import csv
import re
import logging
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

TABELA = "tim_pre_controle"


def _sanitizar_coluna(nome: str) -> str:
    """Converte nome de coluna para identificador SQL válido."""
    s = str(nome).strip()
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    s = s[:64] if len(s) > 64 else s
    return s.lower() if s else f"col_{hash(nome) % 10000}"


def _detectar_delimitador(content: str) -> str:
    """Detecta delimitador do CSV."""
    first_line = content.split('\n')[0] if content else ''
    if '|' in first_line and first_line.count('|') > first_line.count(',') and first_line.count('|') > first_line.count(';'):
        return '|'
    if ';' in first_line and first_line.count(';') > first_line.count(','):
        return ';'
    return ','


def tem_estrutura_tim_pre_controle(headers: List[str]) -> bool:
    """
    Verifica se o CSV parece ser de TIM PRE CONTROLE pelo cabeçalho.
    Colunas típicas: Número de acesso, Código externo, Migrar plano, Pre x Controle
    """
    cols_norm = [str(h).strip().lower() for h in headers if h]
    palavras_chave = [
        'numero de acesso', 'número de acesso', 'codigo externo', 'código externo',
        'migrar plano', 'pre', 'controle', 'motivo de não ter sido migrado',
        'responsavel pelo processamento', 'data inicial do processamento'
    ]
    return sum(1 for p in palavras_chave if any(p in c for c in cols_norm)) >= 2


def processar_tim_pre_controle(arquivo_path: str, db_path: str) -> Dict[str, Any]:
    """
    Processa CSV de TIM PRE - CONTROLE (Migrar plano Pre x Controle).
    Cria tabela tim_pre_controle a partir do cabeçalho e insere os registros.

    Returns:
        Estatísticas: total_linhas, inseridos, erros, colunas
    """
    stats = {'total_linhas': 0, 'inseridos': 0, 'erros': 0, 'colunas': []}

    path = Path(arquivo_path)
    if not path.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_path}")
        return stats

    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    content = None

    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc, errors='replace') as f:
                content = f.read()
                break
        except (UnicodeDecodeError, LookupError):
            continue

    if not content or not content.strip():
        logger.warning(f"Arquivo vazio: {path.name}")
        return stats

    delimiter = _detectar_delimitador(content)
    import io
    reader = csv.reader(io.StringIO(content), delimiter=delimiter)
    rows = list(reader)

    if not rows:
        return stats

    headers_orig = [str(h).strip() for h in rows[0]]
    headers_sql = [_sanitizar_coluna(h) for h in headers_orig]
    stats['colunas'] = headers_sql

    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABELA} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origem_arquivo TEXT,
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute(f"PRAGMA table_info({TABELA})")
        colunas_existentes = {row[1] for row in cursor.fetchall()}

        for col in headers_sql:
            if col and col not in colunas_existentes and col not in ('id', 'origem_arquivo', 'data_importacao'):
                try:
                    cursor.execute(f"ALTER TABLE {TABELA} ADD COLUMN {col} TEXT")
                    colunas_existentes.add(col)
                    logger.debug(f"Coluna adicionada: {col}")
                except sqlite3.OperationalError as e:
                    if "duplicate column" not in str(e).lower():
                        logger.warning(f"Não foi possível adicionar coluna {col}: {e}")

        colunas_insert = [c for c in headers_sql if c and c in colunas_existentes]
        colunas_insert.append('origem_arquivo')
        placeholders = ', '.join(['?' for _ in colunas_insert])
        cols_str = ', '.join(colunas_insert)

        for i, row in enumerate(rows[1:], start=2):
            try:
                if len(row) < len(headers_orig):
                    row.extend([''] * (len(headers_orig) - len(row)))
                valores_row = [str(row[j]).strip()[:1000] if j < len(row) else '' for j in range(len(headers_sql))]
                valores_finais = []
                for c in colunas_insert:
                    if c == 'origem_arquivo':
                        valores_finais.append(path.name)
                    elif c in headers_sql:
                        idx = headers_sql.index(c)
                        valores_finais.append(valores_row[idx] if idx < len(valores_row) else '')
                    else:
                        valores_finais.append('')
                cursor.execute(f"INSERT INTO {TABELA} ({cols_str}) VALUES ({placeholders})", valores_finais)
                stats['inseridos'] += 1
            except Exception as e:
                logger.warning(f"Erro linha {i}: {e}")
                stats['erros'] += 1

        stats['total_linhas'] = len(rows) - 1
        conn.commit()
        logger.info(f"{TABELA}: {stats['inseridos']} registros de {path.name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao processar TIM PRE CONTROLE: {e}")
        raise
    finally:
        conn.close()

    return stats


if __name__ == "__main__":
    import sys
    from config import DB_PATH, PASTA_IMPORTACOES

    pasta = Path(PASTA_IMPORTACOES)
    db = DB_PATH

    arquivos = list(pasta.glob("*Migrar*plano*.csv")) + list(pasta.glob("*Pre*Controle*.csv")) + list(pasta.glob("*pre*controle*.csv"))
    if not arquivos:
        arquivos = [f for f in pasta.glob("*.csv") if "migrar" in f.name.lower() or ("pre" in f.name.lower() and "controle" in f.name.lower())]

    if sys.argv[1:]:
        arquivos = [Path(p) for p in sys.argv[1:]]

    for arq in arquivos:
        if arq.exists():
            st = processar_tim_pre_controle(str(arq), db)
            print(f"  {arq.name}: {st['inseridos']} inseridos, {st['erros']} erros")
