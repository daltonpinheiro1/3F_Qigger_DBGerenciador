"""
Script para limpar registros órfãos do banco de dados
Remove registros em decision_history e rules_log que referenciam portabilidade_records inexistentes
"""
import sys
from pathlib import Path

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
from src.database.db_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/limpeza_orfaos.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def limpar_registros_orfaos():
    """Limpa registros órfãos do banco de dados"""
    print("=" * 70)
    print("LIMPEZA DE REGISTROS ÓRFÃOS")
    print("=" * 70)
    print()
    
    db_path = "data/portabilidade.db"
    
    if not Path(db_path).exists():
        print(f"⚠ Banco de dados não encontrado: {db_path}")
        return 1
    
    print(f"[1] Conectando ao banco: {db_path}")
    db_manager = DatabaseManager(db_path)
    
    # Validar antes
    print("\n[2] Validando integridade antes da limpeza...")
    integrity_before = db_manager.validate_database_integrity()
    
    orphaned_before = sum(integrity_before['orphaned_records'].values())
    print(f"    >> Registros órfãos encontrados: {orphaned_before}")
    
    if orphaned_before == 0:
        print("\n✓ Nenhum registro órfão encontrado. Banco está limpo!")
        return 0
    
    # Limpar decision_history
    print("\n[3] Limpando registros órfãos em decision_history...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Contar antes
        cursor.execute("""
            SELECT COUNT(*) FROM decision_history dh
            LEFT JOIN portabilidade_records pr ON dh.record_id = pr.id
            WHERE pr.id IS NULL
        """)
        count_before = cursor.fetchone()[0]
        
        if count_before > 0:
            # Deletar órfãos
            cursor.execute("""
                DELETE FROM decision_history
                WHERE record_id NOT IN (SELECT id FROM portabilidade_records)
            """)
            deleted_dh = cursor.rowcount
            conn.commit()
            print(f"    >> {deleted_dh} registros removidos de decision_history")
        else:
            print("    >> Nenhum registro órfão em decision_history")
    
    # Limpar rules_log
    print("\n[4] Limpando registros órfãos em rules_log...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Contar antes
        cursor.execute("""
            SELECT COUNT(*) FROM rules_log rl
            LEFT JOIN portabilidade_records pr ON rl.record_id = pr.id
            WHERE pr.id IS NULL
        """)
        count_before = cursor.fetchone()[0]
        
        if count_before > 0:
            # Deletar órfãos
            cursor.execute("""
                DELETE FROM rules_log
                WHERE record_id NOT IN (SELECT id FROM portabilidade_records)
            """)
            deleted_rl = cursor.rowcount
            conn.commit()
            print(f"    >> {deleted_rl} registros removidos de rules_log")
        else:
            print("    >> Nenhum registro órfão em rules_log")
    
    # Validar depois
    print("\n[5] Validando integridade após limpeza...")
    integrity_after = db_manager.validate_database_integrity()
    
    orphaned_after = sum(integrity_after['orphaned_records'].values())
    print(f"    >> Registros órfãos restantes: {orphaned_after}")
    
    # Estatísticas
    print("\n" + "=" * 70)
    print("RESUMO DA LIMPEZA")
    print("=" * 70)
    print(f"  Registros órfãos antes: {orphaned_before}")
    print(f"  Registros órfãos depois: {orphaned_after}")
    print(f"  Total removido: {orphaned_before - orphaned_after}")
    
    if integrity_after['foreign_keys'] == 'OK':
        print("\n✓ Foreign keys agora estão OK!")
    else:
        print(f"\n⚠ Ainda há problemas com foreign keys")
    
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    sys.exit(limpar_registros_orfaos())

