"""
Script para consultar dados do banco unificado
Versão 1.0

Permite consultar registros, histórico de versões e estatísticas do banco unificado
"""

import sys
from pathlib import Path
from datetime import datetime

from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import io

Path('logs').mkdir(exist_ok=True)

if sys.platform == 'win32':
    try:
        console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
    except Exception:
        console_handler = logging.StreamHandler(sys.stdout)
else:
    console_handler = logging.StreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[console_handler]
)

logger = logging.getLogger(__name__)

from src.database.unified_db import UnifiedDatabaseManager

UNIFIED_DB = "data/tim_unificado.db"


def consultar_por_id_isize(id_isize: str):
    """Consulta registro por ID iSize"""
    db = UnifiedDatabaseManager(UNIFIED_DB)
    
    print(f"\n{'='*70}")
    print(f"CONSULTA: ID iSize = {id_isize}")
    print(f"{'='*70}\n")
    
    # Versão mais recente
    record = db.get_latest_record(id_isize)
    if not record:
        print(f"Registro não encontrado: {id_isize}")
        return
    
    print("VERSÃO MAIS RECENTE:")
    print(f"  - Versão: {record.get('versao')}")
    print(f"  - Data Armazenamento: {record.get('data_armazenamento')}")
    print(f"  - Origem: {record.get('origem_dados')}")
    print()
    
    # Campos principais
    print("DADOS PRINCIPAIS:")
    print(f"  - Número da Ordem: {record.get('numero_ordem')}")
    print(f"  - CPF: {record.get('cpf')}")
    print(f"  - Cliente: {record.get('cliente_nome')}")
    print()
    
    print("STATUS (FOCO):")
    print(f"  - Status Ordem: {record.get('status_ordem')}")
    print(f"  - Status Logística: {record.get('status_logistica')}")
    print(f"  - Status Bilhete: {record.get('status_bilhete')}")
    print()
    
    print("MOTIVOS:")
    print(f"  - Motivo Recusa: {record.get('motivo_recusa')}")
    print(f"  - Motivo Cancelamento: {record.get('motivo_cancelamento')}")
    print()
    
    # Histórico
    history = db.get_record_history(id_isize)
    if len(history) > 1:
        print(f"\nHISTÓRICO DE VERSÕES ({len(history)} versões):")
        for h in history:
            print(f"  - Versão {h['versao']}: {h['data_armazenamento']} (origem: {h['origem_dados']})")
            if h['status_ordem'] != record.get('status_ordem'):
                print(f"    Status Ordem: {h.get('status_ordem')} → {record.get('status_ordem')}")
            if h['status_logistica'] != record.get('status_logistica'):
                print(f"    Status Logística: {h.get('status_logistica')} → {record.get('status_logistica')}")


def consultar_por_status(status_ordem=None, status_logistica=None, status_bilhete=None, limit=10):
    """Consulta registros por status"""
    db = UnifiedDatabaseManager(UNIFIED_DB)
    
    print(f"\n{'='*70}")
    print("CONSULTA POR STATUS")
    print(f"{'='*70}\n")
    
    if status_ordem:
        print(f"  Status Ordem: {status_ordem}")
    if status_logistica:
        print(f"  Status Logística: {status_logistica}")
    if status_bilhete:
        print(f"  Status Bilhete: {status_bilhete}")
    print()
    
    records = db.get_records_by_status(
        status_ordem=status_ordem,
        status_logistica=status_logistica,
        status_bilhete=status_bilhete,
        limit=limit
    )
    
    print(f"Registros encontrados: {len(records)}\n")
    
    for i, record in enumerate(records[:limit], 1):
        print(f"{i}. ID: {record.get('id_isize')} | Ordem: {record.get('numero_ordem')}")
        print(f"   Status Ordem: {record.get('status_ordem')} | Logística: {record.get('status_logistica')} | Bilhete: {record.get('status_bilhete')}")
        print(f"   Cliente: {record.get('cliente_nome')}")
        print()


def estatisticas():
    """Exibe estatísticas do banco"""
    db = UnifiedDatabaseManager(UNIFIED_DB)
    
    print(f"\n{'='*70}")
    print("ESTATÍSTICAS DO BANCO UNIFICADO")
    print(f"{'='*70}\n")
    
    with db._get_connection() as conn:
        cursor = conn.cursor()
        
        # Total de registros únicos
        cursor.execute("SELECT COUNT(DISTINCT id_isize) FROM tim_unificado WHERE is_latest = 1")
        total_unicos = cursor.fetchone()[0]
        
        # Total de versões
        cursor.execute("SELECT COUNT(*) FROM tim_unificado")
        total_versoes = cursor.fetchone()[0]
        
        # Por origem
        cursor.execute("""
            SELECT origem_dados, COUNT(*) 
            FROM tim_unificado 
            WHERE is_latest = 1
            GROUP BY origem_dados
        """)
        por_origem = cursor.fetchall()
        
        # Por status ordem
        cursor.execute("""
            SELECT status_ordem, COUNT(*) 
            FROM tim_unificado 
            WHERE is_latest = 1 AND status_ordem IS NOT NULL
            GROUP BY status_ordem
            ORDER BY COUNT(*) DESC
        """)
        por_status_ordem = cursor.fetchall()
        
        # Por status logística
        cursor.execute("""
            SELECT status_logistica, COUNT(*) 
            FROM tim_unificado 
            WHERE is_latest = 1 AND status_logistica IS NOT NULL
            GROUP BY status_logistica
            ORDER BY COUNT(*) DESC
        """)
        por_status_log = cursor.fetchall()
        
        print(f"Total de registros únicos: {total_unicos}")
        print(f"Total de versões (com histórico): {total_versoes}")
        print(f"Média de versões por registro: {total_versoes/total_unicos:.2f}" if total_unicos > 0 else "")
        print()
        
        print("POR ORIGEM DOS DADOS:")
        for origem, count in por_origem:
            print(f"  - {origem}: {count}")
        print()
        
        print("POR STATUS DA ORDEM (Top 10):")
        for status, count in por_status_ordem[:10]:
            print(f"  - {status}: {count}")
        print()
        
        print("POR STATUS LOGÍSTICA (Top 10):")
        for status, count in por_status_log[:10]:
            print(f"  - {status}: {count}")
        print()


def main():
    """Menu principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Consultar banco unificado TIM')
    parser.add_argument('--id', type=str, help='Consultar por ID iSize')
    parser.add_argument('--status-ordem', type=str, help='Filtrar por status da ordem')
    parser.add_argument('--status-logistica', type=str, help='Filtrar por status logística')
    parser.add_argument('--status-bilhete', type=str, help='Filtrar por status do bilhete')
    parser.add_argument('--limit', type=int, default=10, help='Limite de resultados')
    parser.add_argument('--stats', action='store_true', help='Exibir estatísticas')
    
    args = parser.parse_args()
    
    if args.stats:
        estatisticas()
    elif args.id:
        consultar_por_id_isize(args.id)
    else:
        consultar_por_status(
            status_ordem=args.status_ordem,
            status_logistica=args.status_logistica,
            status_bilhete=args.status_bilhete,
            limit=args.limit
        )


if __name__ == "__main__":
    main()

