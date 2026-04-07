"""
Processador dedicado para o banco de dados v2.

Monitora a pasta data/v2/entrada/, identifica automaticamente o tipo de cada
arquivo (CSV/Excel), importa para o banco v2 normalizado e move para Lixeira.

Uso:
    python processar_v2.py                          # Processa todos os arquivos
    python processar_v2.py --workers 3              # Processa com 3 workers paralelos
    python processar_v2.py --arquivo caminho.csv    # Processa um arquivo específico
    python processar_v2.py --validar                # Apenas valida integridade do banco
    python processar_v2.py --stats                  # Mostra estatísticas do banco
    python processar_v2.py --unificada              # Mostra dados da tabela unificada
    python processar_v2.py --no-caffeinate          # Desativa caffeinate no macOS

    # Com venv:
    .venv/bin/python processar_v2.py
"""
import argparse
import json
import logging
import os
import platform
import shutil
import signal
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.database.db_manager_v2 import DatabaseManagerV2
from src.database.importador import Importador
from src.database.data_unifier import DataUnifier
from src.api.abr_telecom import ABRTelecom

# Configuração
DB_V2_PATH = "data/portabilidade_v2.db"
PASTA_ENTRADA = Path("data/v2/entrada")
PASTA_ERROS = Path("data/v2/erros")

# Workers padrão para 16GB RAM (cada worker usa ~200MB)
MAX_WORKERS_DEFAULT = 3

# Extensões aceitas
EXTENSOES_ACEITAS = {'.csv', '.xlsx', '.xls'}

# Logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/processar_v2.log', encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# Caffeinate (macOS) — impede o Mac de dormir durante processamento
# =============================================================================

_caffeinate_proc = None


def _iniciar_caffeinate():
    """Inicia caffeinate no macOS para impedir sleep durante processamento."""
    global _caffeinate_proc
    if platform.system() != 'Darwin':
        return
    try:
        _caffeinate_proc = subprocess.Popen(
            ['caffeinate', '-dims'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info("caffeinate ativado (PID %d)", _caffeinate_proc.pid)
    except FileNotFoundError:
        logger.warning("caffeinate não encontrado")


def _parar_caffeinate():
    """Para o caffeinate."""
    global _caffeinate_proc
    if _caffeinate_proc:
        _caffeinate_proc.terminate()
        _caffeinate_proc.wait()
        logger.info("caffeinate desativado")
        _caffeinate_proc = None


# =============================================================================
# Mover para Lixeira do macOS
# =============================================================================

def _mover_para_lixeira(arquivo: Path):
    """Move arquivo para a Lixeira do macOS. Fallback: deleta."""
    if platform.system() == 'Darwin':
        try:
            subprocess.run(
                ['osascript', '-e',
                 f'tell application "Finder" to delete POSIX file "{arquivo.resolve()}"'],
                capture_output=True, text=True, timeout=10,
            )
            logger.info("Movido para Lixeira: %s", arquivo.name)
            return
        except Exception as e:
            logger.warning("Falha ao mover para Lixeira: %s", e)

    # Fallback: mover para pasta erros
    PASTA_ERROS.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    destino = PASTA_ERROS / f"{ts}_{arquivo.name}"
    shutil.move(str(arquivo), str(destino))
    logger.info("Movido para erros (fallback): %s", destino)


def _listar_arquivos_entrada() -> list:
    """Lista arquivos válidos na pasta de entrada, ordenados por data de modificação."""
    PASTA_ENTRADA.mkdir(parents=True, exist_ok=True)
    arquivos = []
    for ext in EXTENSOES_ACEITAS:
        arquivos.extend(PASTA_ENTRADA.glob(f"*{ext}"))
    arquivos.sort(key=lambda f: f.stat().st_mtime)
    return arquivos


def _executar_abr_sob_demanda(db: DatabaseManagerV2, limite: int = 200):
    """
    Consulta ABR para números recém-importados que ainda não foram verificados.

    Roda automaticamente após cada processamento. Limita a 200 consultas
    por execução para não sobrecarregar a API.
    """
    import sqlite3

    conn = sqlite3.connect(db.db_path)

    # Criar tabela se não existir
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_abr_proposta ON abr_consultas(proposta_isize)")
    conn.commit()

    # Buscar pendentes (propostas sem consulta ABR)
    cursor = conn.cursor()
    cursor.execute("""
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
        LEFT JOIN abr_consultas abr ON abr.proposta_isize = p.proposta_isize
        WHERE abr.id IS NULL
          AND p.cpf IS NOT NULL AND p.cpf != ''
          AND COALESCE(
                NULLIF(TRIM(pt.telefone_portabilidade), ''),
                NULLIF(TRIM(c.telefone_1), ''), ''
              ) != ''
        ORDER BY p.created_at DESC
        LIMIT ?
    """, (limite,))

    pendentes = cursor.fetchall()

    if not pendentes:
        print("  ✓ ABR: nenhum número pendente")
        conn.close()
        return

    print(f"  ABR: {len(pendentes)} números pendentes (limite={limite})")

    abr = ABRTelecom()
    sucesso = 0
    erros_abr = 0
    import json as _json
    import time as _time

    for i, (proposta, cpf, msisdn) in enumerate(pendentes, 1):
        resultado = abr.consultar(cpf, msisdn)

        try:
            cursor.execute(
                """INSERT OR REPLACE INTO abr_consultas
                   (proposta_isize, cpf, msisdn, operadora, ativo,
                    tipo_plano, status_abr, erro, resposta_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    proposta, cpf, msisdn,
                    resultado.get("company"),
                    1 if resultado.get("active") else 0,
                    resultado.get("plan_type"),
                    resultado.get("status"),
                    resultado.get("error"),
                    _json.dumps(resultado.get("raw", {}), ensure_ascii=False),
                ),
            )
            conn.commit()
            if resultado.get("status") == "0":
                sucesso += 1
            else:
                erros_abr += 1
        except Exception:
            erros_abr += 1

        if i < len(pendentes):
            _time.sleep(0.3)

    conn.close()
    print(f"  ✓ ABR: {sucesso} sucesso, {erros_abr} erros")


def _processar_um_arquivo(arquivo_path: str, db_path: str, batch_size: int) -> dict:
    """Processa um único arquivo (executável em worker separado)."""
    arquivo = Path(arquivo_path)
    db = DatabaseManagerV2(db_path)
    importador = Importador(batch_size=batch_size)

    resultado = {
        'arquivo': arquivo.name,
        'caminho': arquivo_path,
        'tipo': '?',
        'inseridos': 0,
        'erros': 0,
        'status': 'erro',
        'duracao': 0,
    }

    inicio = time.time()
    try:
        stats = importador.importar_arquivo(arquivo_path, db)
        resultado['tipo'] = stats.get('tipo_arquivo', '?')
        resultado['inseridos'] = stats.get('inseridos', 0)
        resultado['erros'] = stats.get('erros', 0)
        resultado['status'] = stats.get('status', 'erro')
    except Exception as e:
        resultado['status'] = 'erro'
        resultado['erros'] = 1
        logger.error("Erro ao processar %s: %s", arquivo.name, e)

    resultado['duracao'] = round(time.time() - inicio, 1)
    return resultado


def processar_pasta(db: DatabaseManagerV2, importador: Importador,
                    workers: int = 1, batch_size: int = 100):
    """
    Processa todos os arquivos da pasta de entrada.

    Args:
        db: DatabaseManagerV2 para registro de execução.
        importador: Importador (usado apenas em modo sequencial).
        workers: Número de workers paralelos (1 = sequencial).
        batch_size: Tamanho do lote por worker.
    """
    arquivos = _listar_arquivos_entrada()

    if not arquivos:
        print("Nenhum arquivo encontrado em data/v2/entrada/")
        print(f"Coloque seus arquivos CSV/Excel em: {PASTA_ENTRADA.resolve()}")
        return

    print(f"\n{'='*60}")
    print(f"PROCESSAMENTO V2 — {len(arquivos)} arquivo(s)")
    print(f"{'='*60}")
    print(f"Banco: {DB_V2_PATH}")
    print(f"Entrada: {PASTA_ENTRADA.resolve()}")
    print(f"Workers: {workers}")
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()

    # Registrar execução
    exec_id = db.registrar_execucao('processamento_completo', {
        'arquivos': [a.name for a in arquivos],
        'total': len(arquivos),
    })

    total_inseridos = 0
    total_erros = 0
    resultados = []

    if workers > 1 and len(arquivos) > 1:
        # Processamento paralelo
        print(f"Processando em paralelo com {workers} workers...")
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _processar_um_arquivo,
                    str(arq), DB_V2_PATH, batch_size,
                ): arq
                for arq in arquivos
            }
            for future in as_completed(futures):
                arq = futures[future]
                try:
                    r = future.result()
                except Exception as e:
                    r = {
                        'arquivo': arq.name, 'caminho': str(arq),
                        'tipo': '?', 'inseridos': 0, 'erros': 1,
                        'status': 'erro', 'duracao': 0,
                    }
                    logger.error("Worker falhou para %s: %s", arq.name, e)

                total_inseridos += r['inseridos']
                total_erros += r['erros']
                resultados.append(r)

                emoji = '✓' if r['status'] == 'concluido' else '⚠' if r['status'] == 'duplicado' else '✗'
                print(f"  {emoji} {r['arquivo']}: {r['tipo']} — {r['inseridos']} inseridos, {r['erros']} erros ({r['duracao']}s)")

                # Mover para lixeira ou erros
                arq_path = Path(r['caminho'])
                if arq_path.exists():
                    if r['status'] in ('concluido', 'duplicado'):
                        _mover_para_lixeira(arq_path)
                    else:
                        PASTA_ERROS.mkdir(parents=True, exist_ok=True)
                        ts = datetime.now().strftime('%Y%m%d%H%M%S')
                        shutil.move(str(arq_path), str(PASTA_ERROS / f"{ts}_{arq_path.name}"))
    else:
        # Processamento sequencial
        for i, arquivo in enumerate(arquivos, 1):
            print(f"[{i}/{len(arquivos)}] {arquivo.name}")
            print(f"  Tamanho: {arquivo.stat().st_size / 1024:.1f} KB")

            inicio = time.time()
            try:
                stats = importador.importar_arquivo(str(arquivo), db)
                duracao = time.time() - inicio

                tipo = stats.get('tipo_arquivo', '?')
                inseridos = stats.get('inseridos', 0)
                erros = stats.get('erros', 0)
                status = stats.get('status', 'erro')

                total_inseridos += inseridos
                total_erros += erros

                print(f"  Tipo: {tipo}")
                print(f"  Inseridos: {inseridos}")
                print(f"  Erros: {erros}")
                print(f"  Status: {status}")
                print(f"  Duração: {duracao:.1f}s")

                if status in ('concluido', 'duplicado'):
                    _mover_para_lixeira(arquivo)
                else:
                    PASTA_ERROS.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now().strftime('%Y%m%d%H%M%S')
                    shutil.move(str(arquivo), str(PASTA_ERROS / f"{ts}_{arquivo.name}"))

                resultados.append({
                    'arquivo': arquivo.name,
                    'tipo': tipo,
                    'inseridos': inseridos,
                    'erros': erros,
                    'status': status,
                    'duracao': round(duracao, 1),
                })

            except Exception as e:
                logger.error("Erro ao processar %s: %s", arquivo.name, e)
                print(f"  ✗ ERRO: {e}")
                PASTA_ERROS.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime('%Y%m%d%H%M%S')
                shutil.move(str(arquivo), str(PASTA_ERROS / f"{ts}_{arquivo.name}"))
                total_erros += 1
                resultados.append({
                    'arquivo': arquivo.name,
                    'tipo': '?',
                    'inseridos': 0,
                    'erros': 1,
                    'status': 'erro',
                    'duracao': 0,
                })

            print()

    # Finalizar execução
    status_final = 'concluido' if total_erros == 0 else 'concluido'
    db.finalizar_execucao(exec_id, status_final, total_inseridos, total_erros)

    # Consulta ABR sob demanda (novos números importados)
    if total_inseridos > 0:
        print("\nConsultando ABR Telecom para novos números...")
        try:
            _executar_abr_sob_demanda(db)
        except Exception as e:
            logger.warning("ABR sob demanda falhou (não crítico): %s", e)
            print(f"  ⚠ ABR: {e}")

    # Validar integridade
    print("Validando integridade do banco...")
    integridade = db.validar_integridade()
    if integridade.get('integrity_check') == ['ok']:
        print("  ✓ PRAGMA integrity_check: OK")
    else:
        print(f"  ✗ integrity_check: {integridade.get('integrity_check')}")

    # Resumo
    print(f"\n{'='*60}")
    print("RESUMO DO PROCESSAMENTO")
    print(f"{'='*60}")
    print(f"  Arquivos processados: {len(arquivos)}")
    print(f"  Total inseridos: {total_inseridos:,}")
    print(f"  Total erros: {total_erros:,}")
    print()
    for r in resultados:
        emoji = '✓' if r['status'] in ('concluido',) else '⚠' if r['status'] == 'duplicado' else '✗'
        print(f"  {emoji} {r['arquivo']}: {r['tipo']} — {r['inseridos']} inseridos, {r['erros']} erros ({r['duracao']}s)")
    print(f"{'='*60}")


def processar_arquivo_unico(db: DatabaseManagerV2, importador: Importador, caminho: str):
    """Processa um único arquivo especificado por caminho."""
    arquivo = Path(caminho)
    if not arquivo.exists():
        print(f"Arquivo não encontrado: {caminho}")
        return

    print(f"\n{'='*60}")
    print(f"PROCESSAMENTO V2 — Arquivo único")
    print(f"{'='*60}")
    print(f"Arquivo: {arquivo.name}")
    print(f"Caminho: {arquivo.resolve()}")
    print()

    stats = importador.importar_arquivo(str(arquivo), db)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


def mostrar_unificada(db: DatabaseManagerV2, limite: int = 20):
    """Mostra dados da tabela unificada (vw_base_unificada)."""
    import sqlite3

    print(f"\n{'='*60}")
    print("TABELA UNIFICADA — vw_base_unificada")
    print(f"{'='*60}")
    print("Esta view consolida TODOS os dados de todas as tabelas")
    print("usando proposta_isize como chave principal.")
    print()

    conn = sqlite3.connect(DB_V2_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM vw_base_unificada")
    total = cursor.fetchone()[0]
    print(f"Total de registros na view unificada: {total:,}")

    cursor.execute(f"SELECT * FROM vw_base_unificada LIMIT {limite}")
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]

    print(f"Colunas ({len(cols)}): {', '.join(cols[:10])}...")
    print()

    for i, row in enumerate(rows[:5]):
        d = dict(row)
        print(f"--- Registro {i+1} ---")
        for k, v in d.items():
            if v is not None and str(v).strip():
                print(f"  {k}: {v}")
        print()

    # Cache
    cursor.execute("SELECT COUNT(*) FROM cache_base_unificada")
    cache_count = cursor.fetchone()[0]
    print(f"Cache materializado: {cache_count:,} registros")
    print(f"Para atualizar o cache: DataUnifier(db).reconstruir_cache_completo()")

    conn.close()
    print(f"{'='*60}")


def mostrar_stats(db: DatabaseManagerV2):
    """Mostra estatísticas do banco v2."""
    print(f"\n{'='*60}")
    print("ESTATÍSTICAS DO BANCO V2")
    print(f"{'='*60}")
    print(f"Banco: {DB_V2_PATH}")
    print(f"Tamanho: {Path(DB_V2_PATH).stat().st_size / (1024*1024):.1f} MB")
    print()

    import sqlite3
    conn = sqlite3.connect(DB_V2_PATH)
    cursor = conn.cursor()

    tabelas = [
        ('clientes', 'Clientes'),
        ('propostas', 'Propostas'),
        ('status_venda', 'Status Venda'),
        ('portabilidade', 'Portabilidade'),
        ('portabilidade_tim', 'Portabilidade TIM'),
        ('logistica', 'Logística'),
        ('gross', 'GROSS'),
        ('resultado_gross', 'Resultado GROSS'),
        ('backoffice', 'Backoffice'),
        ('consulta_siebel', 'Consulta Siebel'),
        ('bluechip', 'Bluechip'),
        ('rastreio_entregas', 'Rastreio Entregas'),
        ('servicos_adicionais', 'Serviços Adicionais'),
        ('robo_processamento', 'Robô Processamento'),
        ('decisoes', 'Decisões'),
        ('regras_decisao', 'Regras Decisão'),
        ('auditoria', 'Auditoria'),
        ('lotes_importacao', 'Lotes Importação'),
        ('registros_pendentes', 'Registros Pendentes'),
        ('cache_base_unificada', 'Cache Unificada'),
    ]

    print(f"  {'Tabela':<25s} {'Registros':>12s}")
    print(f"  {'-'*25} {'-'*12}")
    total = 0
    for tabela, nome in tabelas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{tabela}]")
            cnt = cursor.fetchone()[0]
            total += cnt
            print(f"  {nome:<25s} {cnt:>12,}")
        except Exception:
            print(f"  {nome:<25s} {'ERRO':>12s}")

    print(f"  {'-'*25} {'-'*12}")
    print(f"  {'TOTAL':<25s} {total:>12,}")

    # Últimos lotes
    cursor.execute("SELECT id, nome_arquivo, tipo_arquivo, qtd_inseridos, status, created_at FROM lotes_importacao ORDER BY id DESC LIMIT 5")
    rows = cursor.fetchall()
    if rows:
        print(f"\n  Últimos lotes de importação:")
        for r in rows:
            print(f"    #{r[0]} {r[1]} ({r[2]}) — {r[3] or 0} inseridos, status={r[4]}, {r[5]}")

    conn.close()
    print(f"{'='*60}")


def main():
    """Ponto de entrada principal."""
    parser = argparse.ArgumentParser(
        description='Processador v2 — banco normalizado com versionamento.',
    )
    parser.add_argument('--arquivo', type=str, help='Processar um arquivo específico')
    parser.add_argument('--validar', action='store_true', help='Validar integridade do banco')
    parser.add_argument('--stats', action='store_true', help='Estatísticas do banco')
    parser.add_argument('--unificada', action='store_true', help='Mostrar tabela unificada')
    parser.add_argument('--workers', type=int, default=1,
                        help=f'Workers paralelos (padrão: 1, recomendado 16GB: {MAX_WORKERS_DEFAULT})')
    parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do lote (padrão: 100)')
    parser.add_argument('--no-caffeinate', action='store_true', help='Desativar caffeinate no macOS')

    args = parser.parse_args()

    # Caffeinate (macOS)
    if not args.no_caffeinate and not args.validar and not args.stats and not args.unificada:
        _iniciar_caffeinate()

    try:
        db = DatabaseManagerV2(DB_V2_PATH)
        importador = Importador(batch_size=args.batch_size)

        if args.validar:
            integridade = db.validar_integridade()
            print(json.dumps(integridade, indent=2, ensure_ascii=False, default=str))
        elif args.stats:
            mostrar_stats(db)
        elif args.unificada:
            mostrar_unificada(db)
        elif args.arquivo:
            processar_arquivo_unico(db, importador, args.arquivo)
        else:
            processar_pasta(db, importador, workers=args.workers, batch_size=args.batch_size)
    finally:
        _parar_caffeinate()


if __name__ == '__main__':
    main()
