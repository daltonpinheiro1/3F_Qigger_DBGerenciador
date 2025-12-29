"""
Script para validação completa do banco de dados e verificação de backups
"""
import sys
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
from src.database.db_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/validacao_integridade.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def validar_banco_completo():
    """Validação completa do banco de dados"""
    print("=" * 70)
    print("VALIDAÇÃO COMPLETA DO BANCO DE DADOS")
    print("=" * 70)
    print()
    
    db_path = "data/portabilidade.db"
    
    if not Path(db_path).exists():
        print(f"⚠ Banco de dados não encontrado: {db_path}")
        return 1
    
    print(f"[1] Inicializando banco de dados: {db_path}")
    db_manager = DatabaseManager(db_path)
    
    # Verificar versão do schema
    print("\n[2] Verificando versão do schema...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(version) FROM schema_version")
        result = cursor.fetchone()
        current_version = result[0] if result[0] else 0
        print(f"    >> Versão atual: {current_version}")
        print(f"    >> Versão esperada: 5")
        if current_version >= 5:
            print("    >> ✓ Schema atualizado")
        else:
            print(f"    >> ⚠ Schema desatualizado (necessita migração)")
    
    # Validar integridade
    print("\n[3] Validando integridade do banco...")
    integrity = db_manager.validate_database_integrity()
    
    if integrity['integrity_check'] == 'OK':
        print("    >> ✓ Integridade: OK")
    else:
        print(f"    >> ✗ Integridade: {integrity['integrity_check']}")
        if integrity['errors']:
            for error in integrity['errors']:
                print(f"       - {error}")
    
    if integrity['foreign_keys'] == 'OK':
        print("    >> ✓ Foreign Keys: OK")
    else:
        print(f"    >> ⚠ Foreign Keys: {integrity['foreign_keys']}")
        if integrity['errors']:
            for error in integrity['errors']:
                print(f"       - {error}")
    
    if integrity['orphaned_records']:
        print("    >> ⚠ Registros órfãos encontrados:")
        for table, count in integrity['orphaned_records'].items():
            print(f"       - {table}: {count}")
    else:
        print("    >> ✓ Nenhum registro órfão encontrado")
    
    # Estatísticas do banco
    print("\n[4] Estatísticas do banco de dados...")
    size_info = db_manager.get_database_size()
    print(f"    >> Tamanho do arquivo: {size_info['file_size_mb']} MB")
    print(f"    >> Total de linhas: {size_info['total_rows']:,}")
    print(f"    >> Tabelas: {len(size_info['tables'])}")
    print("\n    Detalhamento por tabela:")
    for table, count in sorted(size_info['tables'].items()):
        print(f"       - {table}: {count:,} registros")
    
    # Estatísticas de portabilidade
    print("\n[5] Estatísticas de portabilidade...")
    stats = db_manager.get_statistics()
    print(f"    >> Total de registros: {stats['total_registros']:,}")
    print(f"    >> Registros mapeados: {stats['registros_mapeados']:,}")
    print(f"    >> Registros não mapeados: {stats['registros_nao_mapeados']:,}")
    
    if stats['por_tipo_mensagem']:
        print("\n    Por tipo de mensagem:")
        for tipo, count in sorted(stats['por_tipo_mensagem'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"       - {tipo}: {count:,}")
    
    # Estatísticas de relatório de objetos
    print("\n[6] Estatísticas de Relatório de Objetos...")
    try:
        obj_stats = db_manager.get_relatorio_objetos_stats()
        print(f"    >> Total de versões: {obj_stats['total_versoes']:,}")
        print(f"    >> Registros únicos: {obj_stats['codigos_unicos']:,}")
        print(f"    >> Registros com histórico: {obj_stats['registros_com_historico']:,}")
        print(f"    >> Com ICCID: {obj_stats['com_iccid']:,}")
        print(f"    >> Entregues: {obj_stats['entregues']:,}")
        if obj_stats['ultima_atualizacao']:
            print(f"    >> Última atualização: {obj_stats['ultima_atualizacao']}")
    except Exception as e:
        print(f"    >> ⚠ Erro ao obter estatísticas: {e}")
    
    # Verificar backups
    print("\n[7] Verificando backups...")
    from backup_database import listar_backups
    backups = listar_backups()
    
    if backups:
        print(f"    >> Total de backups: {len(backups)}")
        print(f"    >> Último backup: {backups[0]['name']}")
        print(f"       Data: {backups[0]['date'].strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"       Tamanho: {backups[0]['size_mb']:.2f} MB")
        
        # Verificar se backup é válido
        backup_path = Path(backups[0]['path'])
        if backup_path.exists():
            print("    >> ✓ Backup existe e é acessível")
        else:
            print("    >> ✗ Backup não encontrado")
    else:
        print("    >> ⚠ Nenhum backup encontrado")
    
    # Resumo final
    print("\n" + "=" * 70)
    print("RESUMO DA VALIDAÇÃO")
    print("=" * 70)
    
    problemas = []
    if integrity['integrity_check'] != 'OK':
        problemas.append("Integridade comprometida")
    if integrity['foreign_keys'] != 'OK':
        problemas.append("Problemas com foreign keys")
    if integrity['orphaned_records']:
        problemas.append("Registros órfãos encontrados")
    if current_version < 5:
        problemas.append("Schema desatualizado")
    
    if problemas:
        print("⚠ PROBLEMAS ENCONTRADOS:")
        for problema in problemas:
            print(f"   - {problema}")
    else:
        print("✓ Banco de dados está íntegro e funcionando corretamente!")
    
    print("=" * 70)
    
    return 0 if not problemas else 1

if __name__ == "__main__":
    sys.exit(validar_banco_completo())

