"""
Script para otimizar e atualizar o banco de dados
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
        logging.FileHandler('logs/otimizacao.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def otimizar_banco():
    """Otimiza e atualiza o banco de dados"""
    print("=" * 70)
    print("OTIMIZAÇÃO E ATUALIZAÇÃO DO BANCO DE DADOS")
    print("=" * 70)
    print()
    
    db_path = "data/portabilidade.db"
    
    if not Path(db_path).exists():
        print(f"⚠ Banco de dados não encontrado: {db_path}")
        return 1
    
    print(f"[1] Conectando ao banco: {db_path}")
    db_manager = DatabaseManager(db_path)
    
    # Estatísticas antes
    print("\n[2] Estatísticas antes da otimização...")
    size_before = db_manager.get_database_size()
    print(f"    >> Tamanho: {size_before['file_size_mb']:.2f} MB")
    print(f"    >> Total de registros: {size_before['total_rows']:,}")
    
    # Reconstruir índices
    print("\n[3] Reconstruindo índices...")
    try:
        db_manager.rebuild_indexes()
        print("    >> ✓ Índices reconstruídos com sucesso")
    except Exception as e:
        print(f"    >> ⚠ Erro ao reconstruir índices: {e}")
    
    # Aplicar VACUUM
    print("\n[4] Aplicando VACUUM para otimizar espaço...")
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("VACUUM")
            conn.commit()
        print("    >> ✓ VACUUM executado com sucesso")
    except Exception as e:
        print(f"    >> ⚠ Erro ao executar VACUUM: {e}")
    
    # Aplicar ANALYZE
    print("\n[5] Executando ANALYZE para atualizar estatísticas...")
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("ANALYZE")
            conn.commit()
        print("    >> ✓ ANALYZE executado com sucesso")
    except Exception as e:
        print(f"    >> ⚠ Erro ao executar ANALYZE: {e}")
    
    # Reaplicar otimizações de performance
    print("\n[6] Reaplicando otimizações de performance...")
    try:
        db_manager._apply_performance_optimizations()
        print("    >> ✓ Otimizações reaplicadas")
    except Exception as e:
        print(f"    >> ⚠ Erro ao reaplicar otimizações: {e}")
    
    # Estatísticas depois
    print("\n[7] Estatísticas após otimização...")
    size_after = db_manager.get_database_size()
    print(f"    >> Tamanho: {size_after['file_size_mb']:.2f} MB")
    print(f"    >> Total de registros: {size_after['total_rows']:,}")
    
    # Validar integridade
    print("\n[8] Validando integridade...")
    integrity = db_manager.validate_database_integrity()
    
    if integrity['integrity_check'] == 'OK' and integrity['foreign_keys'] == 'OK':
        print("    >> ✓ Integridade: OK")
        print("    >> ✓ Foreign Keys: OK")
    else:
        print(f"    >> ⚠ Integridade: {integrity['integrity_check']}")
        print(f"    >> ⚠ Foreign Keys: {integrity['foreign_keys']}")
    
    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO DA OTIMIZAÇÃO")
    print("=" * 70)
    
    size_diff = size_after['file_size_mb'] - size_before['file_size_mb']
    if size_diff < 0:
        print(f"  ✓ Espaço liberado: {abs(size_diff):.2f} MB")
    elif size_diff > 0:
        print(f"  ℹ Aumento de tamanho: {size_diff:.2f} MB (normal após otimização)")
    else:
        print(f"  ℹ Tamanho mantido: {size_before['file_size_mb']:.2f} MB")
    
    print(f"  Total de registros: {size_after['total_rows']:,}")
    
    if integrity['integrity_check'] == 'OK' and integrity['foreign_keys'] == 'OK':
        print("\n✓ Banco de dados otimizado e íntegro!")
    else:
        print("\n⚠ Banco otimizado, mas há problemas de integridade")
    
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    sys.exit(otimizar_banco())

