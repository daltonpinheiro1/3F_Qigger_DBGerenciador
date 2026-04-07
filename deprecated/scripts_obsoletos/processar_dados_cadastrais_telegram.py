"""
Processa arquivo de dados cadastrais Telegram (Excel ou CSV)
e insere/atualiza na tabela dados_cadastrais_telegram do portabilidade.db

Arquivos procurados em PASTA_IMPORTACOES (e data/entrada):
- *telegram*.xlsx, *telegram*.csv
- *cadastr*telegram*, *dados*telegram*
"""
import sys
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List
import re

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/processar_dados_cadastrais_telegram.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    from config import DB_PATH, PASTA_IMPORTACOES, PASTA_ENTRADA
    PASTA_IMPORTACOES = Path(PASTA_IMPORTACOES)
    PASTA_ENTRADA = Path(PASTA_ENTRADA)
except ImportError:
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")
    PASTA_IMPORTACOES = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    PASTA_ENTRADA = Path(__file__).parent / "data" / "entrada"


def _limpar_telefone(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    s = ''.join(c for c in str(val) if c.isdigit())
    return s[:11] if s else None


def _limpar_texto(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and str(val) == 'nan'):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ('nan', 'none', '') else None


def _encontrar_coluna(row_or_columns, nomes_possiveis: List[str]):
    """Retorna o valor (ou nome da coluna) que bate com um dos nomes possíveis."""
    if hasattr(row_or_columns, 'index'):
        cols = list(row_or_columns.index)
    else:
        cols = list(row_or_columns)
    cols_lower = [str(c).lower() for c in cols]
    for nome in nomes_possiveis:
        n = nome.lower()
        for i, c in enumerate(cols_lower):
            if n in c or c in n:
                return cols[i]
    return None


def _criar_tabela(conn) -> None:
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dados_cadastrais_telegram (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telefone TEXT,
            telegram_user_id TEXT,
            username TEXT,
            primeiro_nome TEXT,
            nome_completo TEXT,
            cpf TEXT,
            codigo_externo TEXT,
            proposta_isize TEXT,
            data_cadastro TEXT,
            origem_arquivo TEXT,
            data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(telefone, telegram_user_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_telefone ON dados_cadastrais_telegram(telefone)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_user_id ON dados_cadastrais_telegram(telegram_user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_cpf ON dados_cadastrais_telegram(cpf)")
    conn.commit()
    logger.info("Tabela dados_cadastrais_telegram criada/verificada")


def _mapear_linha(row, columns: List[str]) -> Optional[Dict[str, Any]]:
    def get(key_candidates, clean=None):
        col = _encontrar_coluna(columns, key_candidates)
        if col is None:
            return None
        val = row.get(col)
        if clean:
            return clean(val)
        return _limpar_texto(val)

    telefone = get(
        ['telefone', 'Telefone', 'numero', 'Numero', 'celular', 'Celular', 'phone', 'fone'],
        _limpar_telefone
    )
    user_id = get(
        ['telegram_user_id', 'user_id', 'User ID', 'id_telegram', 'telegram id', 'chat_id'],
        lambda x: _limpar_texto(x) if x is not None else None
    )
    username = get(['username', 'Username', 'user_name', 'usuario'])
    primeiro_nome = get(['primeiro_nome', 'Primeiro Nome', 'nome', 'Nome', 'first_name', 'first name'])
    nome_completo = get(['nome_completo', 'Nome Completo', 'full_name', 'nome completo'])
    cpf = get(['cpf', 'CPF', 'documento', 'Documento'])
    codigo_externo = get(['codigo_externo', 'Codigo Externo', 'codigo', 'proposta', 'login externo'])
    proposta_isize = get(['proposta_isize', 'Proposta iSize', 'proposta isize'])
    data_cadastro = get(['data_cadastro', 'Data Cadastro', 'data', 'Data', 'data_criacao'])

    if not telefone and not user_id:
        return None

    return {
        'telefone': telefone or '',
        'telegram_user_id': user_id or '',
        'username': username,
        'primeiro_nome': primeiro_nome,
        'nome_completo': nome_completo,
        'cpf': cpf,
        'codigo_externo': codigo_externo,
        'proposta_isize': proposta_isize,
        'data_cadastro': data_cadastro,
    }


def _encontrar_arquivo_telegram(pasta: Path) -> Optional[Path]:
    """Encontra o arquivo mais recente de dados cadastrais Telegram."""
    padroes = [
        '*telegram*.xlsx', '*telegram*.csv',
        '*cadastr*telegram*.xlsx', '*cadastr*telegram*.csv',
        '*dados*telegram*.xlsx', '*dados*telegram*.csv',
        '*Dados*Cadastrais*Telegram*.xlsx', '*Dados*Cadastrais*Telegram*.csv',
    ]
    candidatos = []
    for p in padroes:
        candidatos.extend(pasta.glob(p))
    for p in ['*telegram*.xlsx', '*telegram*.csv']:
        candidatos.extend(pasta.glob(p.upper()))
    candidatos = [c for c in candidatos if c.is_file() and not c.name.startswith('~')]
    if not candidatos:
        return None
    return max(candidatos, key=lambda x: x.stat().st_mtime)


def processar_dados_cadastrais_telegram(
    arquivo: Optional[Path] = None,
    db_path: str = DB_PATH,
    mover_para_processados: bool = True,
    pasta_importacoes_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Processa arquivo de dados cadastrais Telegram e grava em portabilidade.db.

    Args:
        arquivo: Caminho do arquivo (se None, busca em PASTA_IMPORTACOES e PASTA_ENTRADA).
        db_path: Caminho do banco.
        mover_para_processados: Se True, move o arquivo para processados após sucesso.
        pasta_importacoes_path: Pasta de importações (default: PASTA_IMPORTACOES).

    Returns:
        Dict com sucesso, arquivo, processados, inseridos, atualizados, erros.
    """
    from src.database import DatabaseManager

    stats = {
        'sucesso': False,
        'arquivo': None,
        'processados': 0,
        'inseridos': 0,
        'atualizados': 0,
        'erros': 0,
    }

    pasta = pasta_importacoes_path or PASTA_IMPORTACOES

    if arquivo is None:
        arquivo = _encontrar_arquivo_telegram(pasta)
        if arquivo is None and PASTA_ENTRADA != pasta and PASTA_ENTRADA.exists():
            arquivo = _encontrar_arquivo_telegram(PASTA_ENTRADA)

    if not arquivo or not Path(arquivo).exists():
        logger.warning("Nenhum arquivo de dados cadastrais Telegram encontrado")
        return stats

    arquivo = Path(arquivo)
    stats['arquivo'] = str(arquivo)
    logger.info(f"Processando: {arquivo.name}")

    try:
        if arquivo.suffix.lower() == '.csv':
            import csv
            with open(arquivo, 'r', encoding='utf-8-sig', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=';')
                if reader.fieldnames:
                    cols = list(reader.fieldnames)
                else:
                    cols = []
                rows = []
                for row in reader:
                    rows.append((row, cols))
        else:
            import pandas as pd
            df = pd.read_excel(arquivo, engine='openpyxl')
            if df.empty:
                logger.warning("Arquivo vazio")
                return stats
            cols = list(df.columns)
            rows = [(row.to_dict(), cols) for _, row in df.iterrows()]
    except Exception as e:
        logger.error(f"Erro ao ler arquivo: {e}")
        stats['erros'] += 1
        return stats

    db = DatabaseManager(db_path)
    with db._get_connection() as conn:
        _criar_tabela(conn)
        cursor = conn.cursor()

        for row_dict, columns in rows:
            try:
                dados = _mapear_linha(row_dict, columns)
                if not dados:
                    continue

                telefone = dados.get('telefone') or ''
                user_id = dados.get('telegram_user_id') or ''

                cursor.execute(
                    """
                    SELECT id FROM dados_cadastrais_telegram
                    WHERE (telefone = ? AND ? != '') OR (telegram_user_id = ? AND ? != '')
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (telefone, telefone, user_id, user_id)
                )
                existente = cursor.fetchone()

                dados['origem_arquivo'] = arquivo.name
                campos = [k for k in dados if dados.get(k) is not None]
                valores = [dados[k] for k in campos]

                if existente:
                    set_campos = [c for c in campos if c != 'origem_arquivo']
                    set_clause = ', '.join([f"{c} = ?" for c in set_campos])
                    set_clause += ", updated_at = ?"
                    set_valores = [dados[c] for c in set_campos]
                    set_valores.append(datetime.now().isoformat())
                    cursor.execute(
                        f"UPDATE dados_cadastrais_telegram SET {set_clause} WHERE id = ?",
                        set_valores + [existente[0]]
                    )
                    stats['atualizados'] += 1
                else:
                    placeholders = ', '.join(['?'] * len(campos))
                    cursor.execute(
                        f"INSERT INTO dados_cadastrais_telegram ({', '.join(campos)}) VALUES ({placeholders})",
                        valores
                    )
                    stats['inseridos'] += 1

                stats['processados'] += 1
            except Exception as e:
                logger.debug(f"Erro ao processar linha: {e}")
                stats['erros'] += 1

        conn.commit()

    stats['sucesso'] = stats['processados'] > 0
    logger.info(f"Processados: {stats['processados']}, Inseridos: {stats['inseridos']}, Atualizados: {stats['atualizados']}, Erros: {stats['erros']}")

    if stats['sucesso'] and mover_para_processados:
        try:
            pasta_processados = pasta / "processados"
            pasta_processados.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            destino = pasta_processados / f"{arquivo.stem}_{timestamp}{arquivo.suffix}"
            shutil.move(str(arquivo), str(destino))
            logger.info(f"Arquivo movido para processados: {destino.name}")
        except Exception as e:
            logger.warning(f"Não foi possível mover para processados: {e}")

    return stats


if __name__ == "__main__":
    print("=" * 70)
    print("PROCESSAMENTO DE DADOS CADASTRAIS TELEGRAM")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    resultado = processar_dados_cadastrais_telegram()
    print()
    print(f"  Processados: {resultado['processados']}")
    print(f"  Inseridos: {resultado['inseridos']}")
    print(f"  Atualizados: {resultado['atualizados']}")
    print(f"  Erros: {resultado['erros']}")
    if resultado['sucesso']:
        print("  Status: Concluído com sucesso")
    else:
        print("  Status: Nenhum arquivo processado ou ocorreram erros")
    print("=" * 70)
    sys.exit(0 if resultado['sucesso'] else 1)
