"""
Script para revisar e reorganizar tabelas do portabilidade.db
- Detecta colunas duplicadas (mesmo nome mais de uma vez)
- Reorganiza tabelas preservando os dados (mantém primeira ocorrência de cada coluna)
- Gera relatório e opcionalmente aplica correções

Uso:
  python revisar_tabelas_db.py              # Apenas inspeciona e gera relatório
  python revisar_tabelas_db.py --corrigir   # Inspeciona e aplica correções (com backup)
  python revisar_tabelas_db.py --dry-run    # Mostra o que seria feito sem alterar
"""
import sqlite3
import sys
import logging
from pathlib import Path
from datetime import datetime
from collections import OrderedDict

# Configurar encoding UTF-8
try:
    from src.utils.console_utils import setup_windows_console
    setup_windows_console()
except ImportError:
    pass

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/revisar_tabelas.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

try:
    from config import DB_PATH
    DB_PATH = str(DB_PATH) if hasattr(DB_PATH, '__str__') else DB_PATH
except ImportError:
    DB_PATH = "/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db"


def get_tables(conn: sqlite3.Connection) -> list:
    """Retorna lista de tabelas (excluindo sqlite_*)"""
    cur = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cur.fetchall()]


def get_table_info(conn: sqlite3.Connection, table: str) -> list:
    """
    Retorna PRAGMA table_info como lista de tuplas.
    Cada tupla: (cid, name, type, notnull, dflt_value, pk)
    """
    cur = conn.execute(f"PRAGMA table_info({table})")
    return cur.fetchall()


def get_create_sql(conn: sqlite3.Connection, table: str) -> str:
    """Retorna o SQL de criação da tabela (sqlite_master)."""
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    row = cur.fetchone()
    return row[0] if row else ""


def detectar_colunas_duplicadas(table_info: list) -> tuple:
    """
    Analisa table_info e retorna (colunas_unicas_ordenadas, duplicatas).
    colunas_unicas_ordenadas: lista de (name, type, notnull, dflt_value, pk, cid)
    mantendo a ordem da primeira ocorrência de cada nome.
    duplicatas: lista de (nome, [cids]) para nomes que aparecem mais de uma vez.
    """
    # Ordem de primeira ocorrência: (name -> (cid, type, notnull, dflt_value, pk))
    first_occurrence = OrderedDict()
    # Nomes que aparecem mais de uma vez: name -> [cid1, cid2, ...]
    duplicates = {}
    for row in table_info:
        cid, name, type_, notnull, dflt_value, pk = row
        if name not in first_occurrence:
            first_occurrence[name] = (cid, type_, notnull, dflt_value, pk)
        else:
            if name not in duplicates:
                duplicates[name] = [first_occurrence[name][0]]
            duplicates[name].append(cid)
    # Lista ordenada: (name, type, notnull, dflt_value, pk, cid_da_primeira)
    unique_ordered = []
    for name, (cid, type_, notnull, dflt_value, pk) in first_occurrence.items():
        unique_ordered.append((name, type_, notnull, dflt_value, pk, cid))
    return unique_ordered, duplicates


def verificar_integridade(db_path: str) -> dict:
    """
    Executa PRAGMA quick_check e PRAGMA integrity_check.
    Retorna {'ok': bool, 'quick_check': str, 'integrity_check': list}.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("PRAGMA quick_check")
        quick_result = cur.fetchone()[0]
        cur = conn.execute("PRAGMA integrity_check")
        integrity_results = [row[0] for row in cur.fetchall()]
        return {
            'ok': quick_result == 'ok' and (len(integrity_results) == 1 and integrity_results[0] == 'ok'),
            'quick_check': quick_result,
            'integrity_check': integrity_results,
        }
    finally:
        conn.close()


def inspecionar_banco(db_path: str) -> dict:
    """
    Inspeciona o banco e retorna um dicionário:
    { table_name: { 'info': table_info, 'unique_columns': [...], 'duplicates': {...}, 'tem_duplicata': bool } }
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    resultado = {}
    try:
        tables = get_tables(conn)
        for table in tables:
            info = get_table_info(conn, table)
            unique_ordered, dups = detectar_colunas_duplicadas(info)
            resultado[table] = {
                'info': info,
                'unique_columns': unique_ordered,
                'duplicates': dups,
                'tem_duplicata': len(dups) > 0,
                'total_cols': len(info),
                'cols_unicas': len(unique_ordered),
            }
    finally:
        conn.close()
    return resultado


def criar_ddl_reorganizada(table: str, unique_columns: list, conn: sqlite3.Connection) -> str:
    """
    Gera DDL para tabela nova com colunas únicas.
    Preserva INTEGER PRIMARY KEY e tipos; constraints UNIQUE/CHECK não são copiados
    (podem ser readicionados depois). Aqui o foco é remover colunas duplicadas.
    """
    col_defs = []
    for name, type_, notnull, dflt_value, pk in [c[:5] for c in unique_columns]:
        safe_name = f'"{name}"' if not name.isidentifier() else name
        part = f"{safe_name} {type_ or 'TEXT'}"
        if pk and 'INTEGER' in (type_ or '').upper():
            part += " PRIMARY KEY AUTOINCREMENT"
        elif notnull:
            part += " NOT NULL"
        if dflt_value is not None:
            part += f" DEFAULT {dflt_value}"
        col_defs.append(part)
    return f"CREATE TABLE {table}_new ({', '.join(col_defs)})"


def corrigir_tabela(conn: sqlite3.Connection, table: str, unique_columns: list, cids_manter: list, dry_run: bool) -> bool:
    """
    Cria tabela nova com colunas únicas, copia dados (primeira ocorrência de cada nome), substitui tabela.
    cids_manter: lista de cids na ordem das colunas da nova tabela.
    """
    if dry_run:
        logger.info(f"[DRY-RUN] Corrigiria tabela: {table}")
        return True
    cur = conn.cursor()
    # 1) Criar nova tabela
    col_defs = []
    for name, type_, notnull, dflt_value, pk in [c[:5] for c in unique_columns]:
        safe_name = f'"{name}"' if not name.isidentifier() else name
        part = f"{safe_name} {type_ or 'TEXT'}"
        if pk and type_ and 'INTEGER' in type_.upper():
            part += " PRIMARY KEY AUTOINCREMENT"
        elif notnull:
            part += " NOT NULL"
        if dflt_value is not None:
            part += f" DEFAULT {dflt_value}"
        col_defs.append(part)
    cur.execute(f"CREATE TABLE {table}_new ({', '.join(col_defs)})")
    # 2) Copiar dados: SELECT * da antiga retorna linhas como tuplas na ordem dos cids
    cur.execute(f"SELECT * FROM [{table}]")
    rows = cur.fetchall()
    placeholders = ",".join(["?"] * len(cids_manter))
    cols_new = [c[0] for c in unique_columns]
    cols_quoted = [f'"{c}"' if not c.isidentifier() else c for c in cols_new]
    insert_sql = f"INSERT INTO {table}_new ({','.join(cols_quoted)}) VALUES ({placeholders})"
    for row in rows:
        new_row = tuple(row[cid] for cid in cids_manter)
        cur.execute(insert_sql, new_row)
    # 3) Remover antiga e renomear
    cur.execute(f"DROP TABLE [{table}]")
    cur.execute(f"ALTER TABLE {table}_new RENAME TO [{table}]")
    logger.info(f"Tabela reorganizada: {table} ({len(rows)} linhas)")
    return True


def recriar_indices_tabela(conn: sqlite3.Connection, table: str, dry_run: bool) -> None:
    """Recria índices que referenciam a tabela (por nome)."""
    cur = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table,)
    )
    for (sql,) in cur.fetchall():
        if "sqlite_autoindex" in (sql or ""):
            continue
        if dry_run:
            logger.info(f"[DRY-RUN] Recriaria índice: {sql[:80]}...")
            continue
        try:
            # Índice foi dropado com a tabela; recriar (removendo IF NOT EXISTS do nome se existir)
            conn.execute(sql)
        except Exception as e:
            logger.warning(f"Não foi possível recriar índice: {e}")


def aplicar_correcoes(db_path: str, backup_path: str, dry_run: bool = False) -> list:
    """
    Para cada tabela com colunas duplicadas, reorganiza e preserva dados.
    Faz backup antes se backup_path for informado.
    Retorna lista de tabelas corrigidas.
    """
    if not dry_run and backup_path:
        import shutil
        Path(backup_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(db_path, backup_path)
        logger.info(f"Backup criado: {backup_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    corrigidas = []
    try:
        inspecao = inspecionar_banco(db_path)
        tabelas_com_dup = [t for t, d in inspecao.items() if d['tem_duplicata']]
        if not tabelas_com_dup:
            logger.info("Nenhuma tabela com colunas duplicadas.")
            return []

        for table in tabelas_com_dup:
            d = inspecao[table]
            unique_columns = d['unique_columns']
            cids_manter = [c[5] for c in unique_columns]
            try:
                corrigir_tabela(conn, table, unique_columns, cids_manter, dry_run)
                corrigidas.append(table)
            except Exception as e:
                logger.error(f"Erro ao corrigir {table}: {e}")
                if not dry_run:
                    conn.rollback()
                    raise
        if not dry_run:
            conn.commit()
            # Índices são recriados na próxima inicialização do DatabaseManager (_create_all_indexes)
            logger.info("Execute o sistema (processar_completo ou outro script) para recriar índices.")
    finally:
        conn.close()
    return corrigidas


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Revisar e reorganizar tabelas do portabilidade.db")
    parser.add_argument("--corrigir", action="store_true", help="Aplicar correções (faz backup antes)")
    parser.add_argument("--dry-run", action="store_true", help="Só mostrar o que seria feito")
    parser.add_argument("--db", default=DB_PATH, help="Caminho do banco (default: config)")
    parser.add_argument("--exportar-schema", action="store_true", help="Exportar schema atual para logs/schema_*.txt")
    args = parser.parse_args()

    db_path = args.db
    if not Path(db_path).exists():
        logger.error(f"Banco não encontrado: {db_path}")
        return 1

    print("=" * 70)
    print("REVISÃO DE TABELAS - portabilidade.db")
    print("=" * 70)
    print(f"Banco: {db_path}")
    print(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")

    inspecao = inspecionar_banco(db_path)
    integridade = verificar_integridade(db_path)

    # Relatório
    print("[1] VERIFICAÇÃO DE INTEGRIDADE")
    print("-" * 70)
    if integridade['ok']:
        print("  quick_check: ok")
        print("  integrity_check: ok")
        print("  Status: OK")
    else:
        print(f"  quick_check: {integridade['quick_check']}")
        for line in integridade['integrity_check'][:20]:
            print(f"  {line}")
        if len(integridade['integrity_check']) > 20:
            print(f"  ... e mais {len(integridade['integrity_check']) - 20} linha(s)")
        print("  Status: ERRO - recomenda-se backup e reparo")
    print()

    print("[2] RESUMO POR TABELA")
    print("-" * 70)
    tabelas_duplicadas = []
    for table in sorted(inspecao.keys()):
        d = inspecao[table]
        total = d['total_cols']
        unicas = d['cols_unicas']
        dup = d['tem_duplicata']
        status = "⚠ COLUNAS DUPLICADAS" if dup else "OK"
        print(f"  {table:<40} colunas: {total} (únicas: {unicas})  {status}")
        if dup:
            tabelas_duplicadas.append(table)
            for nome, cids in d['duplicates'].items():
                print(f"      -> Duplicata: '{nome}' (cids: {cids})")

    # Exportar schema para arquivo (opcional)
    if args.exportar_schema:
        Path("logs").mkdir(exist_ok=True)
        out_path = Path("logs") / f"schema_portabilidade_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"Schema: {db_path}\nData: {datetime.now().isoformat()}\n\n")
            for table in sorted(inspecao.keys()):
                d = inspecao[table]
                f.write(f"=== {table} ({d['total_cols']} colunas) ===\n")
                for c in d['unique_columns']:
                    f.write(f"  {c[0]}  {c[1]}\n")
                f.write("\n")
        print(f"\nSchema exportado: {out_path}")

    if not tabelas_duplicadas:
        print("\nNenhuma coluna duplicada encontrada. Banco consistente.")
        if not args.exportar_schema:
            print("Use --exportar-schema para gerar arquivo com o schema em logs/")
        return 0

    if tabelas_duplicadas:
        print(f"\n[3] TOTAL: {len(tabelas_duplicadas)} tabela(s) com colunas duplicadas.")

    if args.corrigir or args.dry_run:
        backup_dir = Path(db_path).parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = str(backup_dir / f"portabilidade_antes_revisao_{timestamp}.db")
        if args.dry_run:
            print("\n[4] DRY-RUN - Simulando correções (nenhuma alteração)")
            aplicar_correcoes(db_path, None, dry_run=True)
        else:
            print("\n[4] Aplicando correções (backup em data/backups/)...")
            corrigidas = aplicar_correcoes(db_path, backup_path, dry_run=False)
            print(f"    Tabelas reorganizadas: {', '.join(corrigidas)}")
        print("\nConcluído.")
    else:
        print("\n[4] Para corrigir, execute: python revisar_tabelas_db.py --corrigir")
        print("    (Será feito backup em data/backups/ antes de alterar)")

    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
