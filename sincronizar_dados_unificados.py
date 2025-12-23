"""
Script para sincronizar dados unificados de todas as fontes
Versão 1.0

Este script integra dados de:
- Base Analítica Final
- Relatório de Objetos
- Gerenciador/Siebel (portabilidade_records do banco existente)

E armazena no banco unificado com versionamento completo.
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import io

# Configurar logging
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
    handlers=[
        logging.FileHandler('logs/sincronizacao_unificada.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database.unified_db import UnifiedDatabaseManager
from src.database.db_manager import DatabaseManager as PortabilidadeDBManager
from src.utils.data_unifier import DataUnifier

# Caminhos padrão
BASE_ANALITICA_PATH = Path(r"G:\Meu Drive\3F Contact Center\base_analitica_final.csv")
REPORT_FOLDER = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
PORTABILIDADE_DB = "data/portabilidade.db"
UNIFIED_DB = "data/tim_unificado.db"


def encontrar_arquivo_objetos_mais_recente() -> Path:
    """Encontra o arquivo de relatório de objetos mais recente"""
    if not REPORT_FOLDER.exists():
        return None
    
    arquivos_xlsx = list(REPORT_FOLDER.glob("*.xlsx"))
    if not arquivos_xlsx:
        return None
    
    return max(arquivos_xlsx, key=lambda x: x.stat().st_mtime)


def carregar_portabilidade_records(db_path: str):
    """Carrega registros do banco de portabilidade existente"""
    try:
        db_manager = PortabilidadeDBManager(db_path)
        
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM portabilidade_records
                ORDER BY id DESC
            """)
            
            rows = cursor.fetchall()
            logger.info(f"Carregados {len(rows)} registros do banco de portabilidade")
            
            # Converter para PortabilidadeRecord (simplificado - apenas dados necessários)
            from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem
            from datetime import datetime
            
            records = []
            for row in rows:
                try:
                    record = PortabilidadeRecord(
                        cpf=row['cpf'] or '',
                        numero_acesso=row['numero_acesso'] or '',
                        numero_ordem=row['numero_ordem'] or row['codigo_externo'] or '',
                        codigo_externo=row['codigo_externo'] or '',
                    )
                    
                    # Preencher campos opcionais
                    if row.get('status_bilhete'):
                        try:
                            record.status_bilhete = PortabilidadeStatus(row['status_bilhete'])
                        except:
                            pass
                    
                    if row.get('status_ordem'):
                        try:
                            record.status_ordem = StatusOrdem(row['status_ordem'])
                        except:
                            pass
                    
                    # Atribuir campos diretamente do dicionário
                    for key in row.keys():
                        if hasattr(record, key) and row[key] is not None:
                            try:
                                setattr(record, key, row[key])
                            except:
                                pass
                    
                    records.append(record)
                
                except Exception as e:
                    logger.warning(f"Erro ao converter registro: {e}")
                    continue
            
            return records
            
    except Exception as e:
        logger.error(f"Erro ao carregar registros de portabilidade: {e}")
        return []


def main():
    """Função principal de sincronização"""
    
    print("=" * 70)
    print("SINCRONIZAÇÃO DE DADOS UNIFICADOS - TIM")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # 1. Inicializar banco unificado
    print("[1] Inicializando banco de dados unificado...")
    unified_db = UnifiedDatabaseManager(UNIFIED_DB)
    unifier = DataUnifier(unified_db)
    print(f"    >> Banco unificado: {UNIFIED_DB}")
    print()
    
    # 2. Encontrar arquivos
    print("[2] Localizando arquivos de origem...")
    
    arquivo_objetos = encontrar_arquivo_objetos_mais_recente()
    
    print(f"    >> Base Analítica: {BASE_ANALITICA_PATH} ({'✓' if BASE_ANALITICA_PATH.exists() else '✗'})")
    print(f"    >> Relatório Objetos: {arquivo_objetos} ({'✓' if arquivo_objetos and arquivo_objetos.exists() else '✗'})")
    print(f"    >> Banco Portabilidade: {PORTABILIDADE_DB} ({'✓' if Path(PORTABILIDADE_DB).exists() else '✗'})")
    print()
    
    # 3. Sincronizar dados
    print("[3] Sincronizando dados de todas as fontes...")
    print()
    
    # Carregar registros de portabilidade se o banco existir
    portabilidade_records = None
    if Path(PORTABILIDADE_DB).exists():
        print("    >> Carregando registros de portabilidade...")
        portabilidade_records = carregar_portabilidade_records(PORTABILIDADE_DB)
        print(f"    >> {len(portabilidade_records)} registros carregados")
        print()
    
    # Sincronizar todas as fontes
    stats = unifier.synchronize_all_sources(
        base_analitica_path=str(BASE_ANALITICA_PATH) if BASE_ANALITICA_PATH.exists() else None,
        relatorio_objetos_path=str(arquivo_objetos) if arquivo_objetos else None,
        portabilidade_records=portabilidade_records
    )
    
    # 4. Exibir estatísticas
    print()
    print("=" * 70)
    print("RESULTADO DA SINCRONIZAÇÃO")
    print("=" * 70)
    print()
    
    if stats['base_analitica']:
        print("Base Analítica:")
        print(f"  - Processados: {stats['base_analitica']['processados']}")
        print(f"  - Novos: {stats['base_analitica']['novos']}")
        print(f"  - Atualizados: {stats['base_analitica']['atualizados']}")
        print(f"  - Erros: {stats['base_analitica']['erros']}")
        print()
    
    if stats['relatorio_objetos']:
        print("Relatório de Objetos:")
        print(f"  - Processados: {stats['relatorio_objetos']['processados']}")
        print(f"  - Novos: {stats['relatorio_objetos']['novos']}")
        print(f"  - Atualizados: {stats['relatorio_objetos']['atualizados']}")
        print(f"  - Erros: {stats['relatorio_objetos']['erros']}")
        print()
    
    if stats['portabilidade']:
        print("Portabilidade (Gerenciador):")
        print(f"  - Processados: {stats['portabilidade']['processados']}")
        print(f"  - Novos: {stats['portabilidade']['novos']}")
        print(f"  - Atualizados: {stats['portabilidade']['atualizados']}")
        print(f"  - Erros: {stats['portabilidade']['erros']}")
        print()
    
    print("TOTAL:")
    print(f"  - Processados: {stats['total_processados']}")
    print(f"  - Novos: {stats['total_novos']}")
    print(f"  - Atualizados: {stats['total_atualizados']}")
    print(f"  - Erros: {stats['total_erros']}")
    print()
    print("=" * 70)
    print("SINCRONIZAÇÃO CONCLUÍDA!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSincronização interrompida pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal na sincronização: {e}", exc_info=True)
        print(f"\n\nErro fatal: {e}")
        sys.exit(1)

