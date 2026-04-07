"""
Atualiza o banco v2 com dados da ABR Telecom.

Consulta operadora, status (ativo/inativo) e tipo de plano para
números que ainda não têm essa informação no banco.

Uso:
    .venv/bin/python atualizar_abr_v2.py                # Atualiza todos pendentes
    .venv/bin/python atualizar_abr_v2.py --limite 100   # Limita a 100 consultas
    .venv/bin/python atualizar_abr_v2.py --teste        # Testa com 1 número
    .venv/bin/python atualizar_abr_v2.py --stats        # Mostra estatísticas
"""
import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.api.abr_telecom import ABRTelecom
from src.database.db_manager_v2 import DatabaseManagerV2

DB_V2_PATH = "data/portabilidade_v2.db"

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/atualizar_abr.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def _criar_tabela_abr(conn: sqlite3.Connection):
    """Cria tabela para armazenar resultados ABR se não existir."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS abr_consultas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposta_isize TEXT NOT NULL,
            cpf TEXT,
            msisdn TEXT NOT NULL,
            operadora TEXT,
            ativo INTEGER,
            tipo_plano TEXT,
            status_abr TEXT,
            erro TEXT,
            resposta_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(proposta_isize, msisdn)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_abr_proposta
        ON abr_consultas(proposta_isize)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_abr_msisdn
        ON abr_consultas(msisdn)
    """)
    conn.commit()


def buscar_pendentes(conn: sqlite3.Connection, limite: int = 0) -> list:
    """Busca propostas com telefone que ainda não foram consultadas na ABR."""
    query = """
        SELECT DISTINCT
            p.proposta_isize,
            p.cpf,
            COALESCE(
                NULLIF(TRIM(pt.telefone_portabilidade), ''),
                NULLIF(TRIM(c.telefone_1), ''),
                ''
            ) AS msisdn
        FROM propostas p
        INNER JOIN (
            SELECT proposta_isize, MAX(versao) AS mv
            FROM propostas GROUP BY proposta_isize
        ) pm ON p.proposta_isize = pm.proposta_isize AND p.versao = pm.mv
        LEFT JOIN clientes c ON p.cpf = c.cpf
            AND c.versao = (SELECT MAX(versao) FROM clientes WHERE cpf = p.cpf)
        LEFT JOIN portabilidade pt ON p.proposta_isize = pt.proposta_isize
            AND pt.versao = (SELECT MAX(versao) FROM portabilidade
                             WHERE proposta_isize = p.proposta_isize)
        LEFT JOIN abr_consultas abr
            ON abr.proposta_isize = p.proposta_isize
        WHERE abr.id IS NULL
          AND p.cpf IS NOT NULL AND p.cpf != ''
          AND COALESCE(
                NULLIF(TRIM(pt.telefone_portabilidade), ''),
                NULLIF(TRIM(c.telefone_1), ''),
                ''
              ) != ''
        ORDER BY p.data_venda DESC
    """
    if limite > 0:
        query += f" LIMIT {limite}"

    cursor = conn.cursor()
    cursor.execute(query)
    return [
        {"proposta_isize": r[0], "cpf": r[1], "msisdn": r[2]}
        for r in cursor.fetchall()
    ]


def atualizar(limite: int = 0, delay: float = 0.3):
    """Executa a atualização ABR para propostas pendentes."""
    conn = sqlite3.connect(DB_V2_PATH)
    _criar_tabela_abr(conn)

    pendentes = buscar_pendentes(conn, limite)
    total = len(pendentes)

    if total == 0:
        print("Nenhuma proposta pendente para consulta ABR.")
        conn.close()
        return

    print(f"\n{'='*60}")
    print(f"ATUALIZAÇÃO ABR TELECOM")
    print(f"{'='*60}")
    print(f"Pendentes: {total:,}")
    print(f"Delay: {delay}s entre consultas")
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()

    abr = ABRTelecom()
    cursor = conn.cursor()

    sucesso = 0
    erros = 0
    inicio = time.time()

    for i, reg in enumerate(pendentes, 1):
        proposta = reg["proposta_isize"]
        cpf = reg["cpf"]
        msisdn = reg["msisdn"]

        resultado = abr.consultar(cpf, msisdn)

        try:
            cursor.execute(
                """INSERT OR REPLACE INTO abr_consultas
                   (proposta_isize, cpf, msisdn, operadora, ativo,
                    tipo_plano, status_abr, erro, resposta_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    proposta,
                    cpf,
                    msisdn,
                    resultado.get("company"),
                    1 if resultado.get("active") else 0,
                    resultado.get("plan_type"),
                    resultado.get("status"),
                    resultado.get("error"),
                    json.dumps(resultado.get("raw", {}), ensure_ascii=False),
                ),
            )
            conn.commit()

            if resultado.get("status") == "0":
                sucesso += 1
            else:
                erros += 1

        except Exception as e:
            logger.error("Erro ao salvar ABR para %s: %s", proposta, e)
            erros += 1

        if i % 50 == 0:
            elapsed = time.time() - inicio
            rate = i / elapsed if elapsed > 0 else 0
            print(
                f"  [{i}/{total}] sucesso={sucesso}, erros={erros}, "
                f"vel={rate:.1f}/s"
            )

        if delay > 0 and i < total:
            time.sleep(delay)

    conn.close()
    duracao = time.time() - inicio

    print(f"\n{'='*60}")
    print(f"RESUMO")
    print(f"{'='*60}")
    print(f"  Consultados: {total:,}")
    print(f"  Sucesso: {sucesso:,}")
    print(f"  Erros: {erros:,}")
    print(f"  Duração: {duracao:.1f}s")
    print(f"{'='*60}")


def mostrar_stats():
    """Mostra estatísticas das consultas ABR."""
    conn = sqlite3.connect(DB_V2_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM abr_consultas")
    except sqlite3.OperationalError:
        print("Tabela abr_consultas não existe. Execute uma atualização primeiro.")
        conn.close()
        return

    total = cursor.fetchone()[0]
    print(f"\n{'='*60}")
    print(f"ESTATÍSTICAS ABR TELECOM")
    print(f"{'='*60}")
    print(f"Total consultas: {total:,}")

    if total > 0:
        cursor.execute("""
            SELECT status_abr, COUNT(*) FROM abr_consultas
            GROUP BY status_abr ORDER BY COUNT(*) DESC
        """)
        print("\nPor status:")
        for r in cursor.fetchall():
            print(f"  {r[0]}: {r[1]:,}")

        cursor.execute("""
            SELECT operadora, COUNT(*) FROM abr_consultas
            WHERE operadora IS NOT NULL
            GROUP BY operadora ORDER BY COUNT(*) DESC
        """)
        print("\nPor operadora:")
        for r in cursor.fetchall():
            print(f"  {r[0]}: {r[1]:,}")

        cursor.execute("""
            SELECT
                SUM(CASE WHEN ativo = 1 THEN 1 ELSE 0 END) as ativos,
                SUM(CASE WHEN ativo = 0 THEN 1 ELSE 0 END) as inativos
            FROM abr_consultas WHERE status_abr = '0'
        """)
        r = cursor.fetchone()
        print(f"\nAtivos: {r[0] or 0:,}")
        print(f"Inativos: {r[1] or 0:,}")

    # Pendentes
    _criar_tabela_abr(conn)
    pendentes = buscar_pendentes(conn)
    print(f"\nPendentes: {len(pendentes):,}")

    conn.close()
    print(f"{'='*60}")


def teste():
    """Testa a conexão com a API ABR com um número de exemplo."""
    print("Testando conexão ABR Telecom...")
    abr = ABRTelecom()

    # Buscar primeiro número do banco
    conn = sqlite3.connect(DB_V2_PATH)
    _criar_tabela_abr(conn)
    pendentes = buscar_pendentes(conn, limite=1)
    conn.close()

    if not pendentes:
        print("Nenhum número pendente para teste.")
        return

    reg = pendentes[0]
    print(f"Consultando: CPF={reg['cpf'][:3]}***, MSISDN={reg['msisdn'][:4]}***")

    resultado = abr.consultar(reg["cpf"], reg["msisdn"])
    print(f"\nResultado:")
    print(json.dumps(resultado, indent=2, ensure_ascii=False, default=str))


def main():
    """Ponto de entrada."""
    parser = argparse.ArgumentParser(
        description="Atualiza banco v2 com dados ABR Telecom"
    )
    parser.add_argument(
        "--limite", type=int, default=0, help="Limite de consultas (0=todas)"
    )
    parser.add_argument(
        "--delay", type=float, default=0.3, help="Delay entre consultas (s)"
    )
    parser.add_argument("--teste", action="store_true", help="Testar com 1 número")
    parser.add_argument("--stats", action="store_true", help="Mostrar estatísticas")

    args = parser.parse_args()

    if args.teste:
        teste()
    elif args.stats:
        mostrar_stats()
    else:
        atualizar(limite=args.limite, delay=args.delay)


if __name__ == "__main__":
    main()
