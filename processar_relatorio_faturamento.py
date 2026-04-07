"""
Processa arquivos CSV de relatório de faturamento.
Lê o cabeçalho e cria a tabela relatorio_faturamento dinamicamente.
"""
import csv
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

TABELA_FATURAMENTO = "relatorio_faturamento"


def _sanitizar_coluna(nome: str) -> str:
    """Converte nome de coluna para identificador SQL válido."""
    s = str(nome).strip()
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    s = s[:64] if len(s) > 64 else s  # Limite SQLite
    return s.lower() if s else f"col_{hash(nome) % 10000}"


def _detectar_delimitador(content: str) -> str:
    """Detecta delimitador do CSV (virgula, ponto-e-virgula ou pipe)."""
    first_line = content.split('\n')[0] if content else ''
    if '|' in first_line and first_line.count('|') > first_line.count(',') and first_line.count('|') > first_line.count(';'):
        return '|'
    if ';' in first_line and first_line.count(';') > first_line.count(','):
        return ';'
    return ','


def tem_estrutura_faturamento(headers: List[str]) -> bool:
    """
    Verifica se o CSV parece ser de faturamento pelo cabeçalho.
    Colunas típicas: faturamento, fatura, competencia, mes, valor, periodo, etc.
    """
    cols_norm = [str(h).strip().lower() for h in headers if h]
    palavras_chave = [
        'faturamento', 'fatura', 'competencia', 'competência',
        'mes', 'mês', 'ano', 'valor', 'periodo', 'período',
        'proposta', 'codigo', 'código', 'cliente', 'venda',
        'status', 'data', 'referencia'
    ]
    return sum(1 for p in palavras_chave if any(p in c for c in cols_norm)) >= 2


def processar_relatorio_faturamento(arquivo_path: str, db_path: str) -> Dict[str, Any]:
    """
    Processa CSV de relatório/faturamento.
    Cria tabela a partir do cabeçalho e insere os registros.

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
    encoding_usado = None

    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc, errors='replace') as f:
                content = f.read()
                encoding_usado = enc
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
        # Criar tabela se não existir ou adicionar colunas novas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS relatorio_faturamento (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origem_arquivo TEXT,
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("PRAGMA table_info(relatorio_faturamento)")
        colunas_existentes = {row[1] for row in cursor.fetchall()}

        for col in headers_sql:
            if col and col not in colunas_existentes and col not in ('id', 'origem_arquivo', 'data_importacao'):
                try:
                    cursor.execute(f"ALTER TABLE relatorio_faturamento ADD COLUMN {col} TEXT")
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
                cursor.execute(f"INSERT INTO relatorio_faturamento ({cols_str}) VALUES ({placeholders})", valores_finais)
                stats['inseridos'] += 1
            except Exception as e:
                logger.warning(f"Erro linha {i}: {e}")
                stats['erros'] += 1

        stats['total_linhas'] = len(rows) - 1
        conn.commit()
        logger.info(f"relatorio_faturamento: {stats['inseridos']} registros de {path.name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao processar relatório faturamento: {e}")
        raise
    finally:
        conn.close()

    return stats
