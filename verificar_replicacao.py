"""
Script para verificar se todos os dados estão replicados no portabilidade.db
"""
import sys
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
from src.database.db_manager import DatabaseManager
from src.utils.objects_loader import ObjectsLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/verificacao_replicacao.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def verificar_replicacao():
    """Verifica se todos os dados estão replicados no banco"""
    print("=" * 70)
    print("VERIFICAÇÃO DE REPLICAÇÃO - PORTABILIDADE.DB")
    print("=" * 70)
    print()
    
    db_path = "data/portabilidade.db"
    
    if not Path(db_path).exists():
        print(f"⚠ Banco de dados não encontrado: {db_path}")
        return 1
    
    print(f"[1] Conectando ao banco: {db_path}")
    db_manager = DatabaseManager(db_path)
    
    # Estatísticas do banco
    print("\n[2] Estatísticas do banco de dados...")
    size_info = db_manager.get_database_size()
    print(f"    >> Tamanho: {size_info['file_size_mb']:.2f} MB")
    print(f"    >> Total de registros: {size_info['total_rows']:,}")
    
    print("\n    Detalhamento por tabela:")
    for table, count in sorted(size_info['tables'].items()):
        print(f"       - {table}: {count:,} registros")
    
    # Verificar portabilidade_records
    print("\n[3] Verificando dados de Portabilidade...")
    stats = db_manager.get_statistics()
    print(f"    >> Total de registros: {stats['total_registros']:,}")
    print(f"    >> Registros mapeados: {stats['registros_mapeados']:,}")
    print(f"    >> Registros não mapeados: {stats['registros_nao_mapeados']:,}")
    
    # Verificar relatorio_objetos
    print("\n[4] Verificando Relatório de Objetos (logística)...")
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
    
    # Verificar se há arquivos pendentes na pasta de importação
    print("\n[5] Verificando arquivos pendentes na pasta de importação...")
    pasta_importacao = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
    
    if pasta_importacao.exists():
        arquivos_csv = list(pasta_importacao.glob("*.csv"))
        arquivos_objetos = list(pasta_importacao.glob("Relatorio_Objetos*.xlsx"))
        arquivos_objetos.extend(pasta_importacao.glob("*.xlsx"))
        
        print(f"    >> Pasta: {pasta_importacao}")
        print(f"    >> Arquivos CSV encontrados: {len(arquivos_csv)}")
        print(f"    >> Arquivos XLSX (Relatório de Objetos): {len(arquivos_objetos)}")
        
        if arquivos_csv:
            print("\n    Arquivos CSV pendentes:")
            for arquivo in arquivos_csv:
                size_mb = arquivo.stat().st_size / (1024 * 1024)
                mtime = datetime.fromtimestamp(arquivo.stat().st_mtime)
                print(f"       - {arquivo.name} ({size_mb:.2f} MB, {mtime.strftime('%d/%m/%Y %H:%M:%S')})")
        
        if arquivos_objetos:
            print("\n    Arquivos XLSX (Relatório de Objetos) pendentes:")
            for arquivo in arquivos_objetos:
                size_mb = arquivo.stat().st_size / (1024 * 1024)
                mtime = datetime.fromtimestamp(arquivo.stat().st_mtime)
                print(f"       - {arquivo.name} ({size_mb:.2f} MB, {mtime.strftime('%d/%m/%Y %H:%M:%S')})")
            
            # Verificar se o arquivo mais recente já está no banco
            if arquivos_objetos:
                arquivo_mais_recente = max(arquivos_objetos, key=lambda x: x.stat().st_mtime)
                print(f"\n    >> Verificando se o arquivo mais recente já está sincronizado...")
                print(f"       Arquivo: {arquivo_mais_recente.name}")
                
                try:
                    objects_loader = ObjectsLoader(str(arquivo_mais_recente))
                    print(f"       Registros no arquivo: {objects_loader.total_records:,}")
                    
                    # Verificar quantos já estão no banco
                    with db_manager._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(DISTINCT registro_id_base) FROM relatorio_objetos")
                        total_no_banco = cursor.fetchone()[0]
                    
                    print(f"       Registros únicos no banco: {total_no_banco:,}")
                    
                    if objects_loader.total_records <= total_no_banco:
                        print(f"       ✓ Arquivo parece estar sincronizado (banco tem {total_no_banco - objects_loader.total_records:,} registros a mais)")
                    else:
                        print(f"       ⚠ Arquivo tem {objects_loader.total_records - total_no_banco:,} registros que podem não estar no banco")
                        print(f"       >> Recomendação: Execute processar_importacoes.py para sincronizar")
                except Exception as e:
                    print(f"       ⚠ Erro ao verificar arquivo: {e}")
        else:
            print("    >> ✓ Nenhum arquivo pendente")
    else:
        print(f"    >> ⚠ Pasta de importação não encontrada: {pasta_importacao}")
    
    # Verificar integridade
    print("\n[6] Verificando integridade do banco...")
    integrity = db_manager.validate_database_integrity()
    
    if integrity['integrity_check'] == 'OK' and integrity['foreign_keys'] == 'OK':
        print("    >> ✓ Integridade: OK")
        print("    >> ✓ Foreign Keys: OK")
    else:
        print(f"    >> ⚠ Integridade: {integrity['integrity_check']}")
        print(f"    >> ⚠ Foreign Keys: {integrity['foreign_keys']}")
    
    # Resumo final
    print("\n" + "=" * 70)
    print("RESUMO DA VERIFICAÇÃO")
    print("=" * 70)
    
    status_ok = True
    mensagens = []
    
    if stats['total_registros'] == 0:
        status_ok = False
        mensagens.append("⚠ Nenhum registro de portabilidade no banco")
    else:
        mensagens.append(f"✓ {stats['total_registros']:,} registros de portabilidade")
    
    try:
        if obj_stats['total_versoes'] == 0:
            status_ok = False
            mensagens.append("⚠ Nenhum registro de Relatório de Objetos no banco")
        else:
            mensagens.append(f"✓ {obj_stats['total_versoes']:,} versões de Relatório de Objetos")
    except:
        mensagens.append("⚠ Não foi possível verificar Relatório de Objetos")
    
    if arquivos_csv or arquivos_objetos:
        status_ok = False
        mensagens.append(f"⚠ {len(arquivos_csv) + len(arquivos_objetos)} arquivo(s) pendente(s) na pasta de importação")
    else:
        mensagens.append("✓ Nenhum arquivo pendente na pasta de importação")
    
    if integrity['integrity_check'] == 'OK' and integrity['foreign_keys'] == 'OK':
        mensagens.append("✓ Banco íntegro")
    else:
        status_ok = False
        mensagens.append("⚠ Problemas de integridade detectados")
    
    for msg in mensagens:
        print(f"  {msg}")
    
    print("\n" + "=" * 70)
    
    if status_ok:
        print("✓ TUDO ESTÁ REPLICADO E ATUALIZADO NO PORTABILIDADE.DB!")
    else:
        print("⚠ HÁ ITENS PENDENTES OU PROBLEMAS DETECTADOS")
        print("\nRecomendações:")
        if arquivos_csv or arquivos_objetos:
            print("  - Execute: py processar_importacoes.py")
        if integrity['integrity_check'] != 'OK' or integrity['foreign_keys'] != 'OK':
            print("  - Execute: py limpar_registros_orfaos.py")
            print("  - Execute: py otimizar_banco.py")
    
    print("=" * 70)
    
    return 0 if status_ok else 1

if __name__ == "__main__":
    sys.exit(verificar_replicacao())

