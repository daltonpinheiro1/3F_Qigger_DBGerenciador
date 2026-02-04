"""
Script para criar backup do banco de dados antes de reprocessamento
Inclui replicação para pasta de rede (Backoffice)
"""
import sys
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
import logging

# Configurar encoding UTF-8 (opcional - não falha se não encontrar)
try:
    from src.utils.console_utils import setup_windows_console
    setup_windows_console()
except ImportError:
    pass  # Ignorar se não conseguir importar

Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backup.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURAÇÃO DE BACKUP NA REDE (SMB)
# =============================================================================
# Destino: smb://files/07 Backoffice/RETORNOS RPA - QIGGER/db.Portabilidade/portabilidade.db
try:
    from config import DB_PATH, SMB_URL_07_BACKOFFICE, BACKUP_REDE_DIR, BACKUP_REDE_PATH
    DB_PATH_LOCAL = DB_PATH if isinstance(DB_PATH, str) else str(DB_PATH)
    SMB_URL_BACKOFFICE = SMB_URL_07_BACKOFFICE
    BACKUP_REDE_DIR = Path(BACKUP_REDE_DIR) if not isinstance(BACKUP_REDE_DIR, Path) else BACKUP_REDE_DIR
    BACKUP_REDE_PATH = BACKUP_REDE_PATH if isinstance(BACKUP_REDE_PATH, str) else str(BACKUP_REDE_DIR / "portabilidade.db")
except ImportError:
    DB_PATH_LOCAL = "/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db"
    SMB_URL_BACKOFFICE = "smb://files/07 Backoffice"
    BACKUP_REDE_DIR = Path("/Volumes/07 Backoffice/RETORNOS RPA - QIGGER/db.Portabilidade")
    BACKUP_REDE_PATH = str(BACKUP_REDE_DIR / "portabilidade.db")


def backup_sqlite_seguro(src_path: str, dst_path: str) -> bool:
    """
    Cria backup do SQLite usando o comando .backup nativo
    Isso é seguro mesmo com o banco em uso (snapshot consistente)
    
    Args:
        src_path: Caminho do banco de origem
        dst_path: Caminho do backup de destino
        
    Returns:
        True se sucesso, False se falhou
    """
    try:
        # Criar diretório de destino se não existir
        dst_dir = Path(dst_path).parent
        dst_dir.mkdir(parents=True, exist_ok=True)
        
        # Usar sqlite3 .backup para criar um snapshot consistente
        cmd = f'sqlite3 "{src_path}" ".backup \'{dst_path}\'"'
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos de timeout
        )
        
        if result.returncode == 0:
            return True
        else:
            logger.error(f"Erro no sqlite3 backup: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Timeout ao criar backup (5 minutos)")
        return False
    except Exception as e:
        logger.error(f"Erro ao criar backup: {e}")
        return False


def replicar_para_rede(db_path: str = None) -> bool:
    """
    Replica o banco de dados para a pasta de rede (Backoffice)
    Usa sqlite3 .backup para garantir consistência, depois copia para rede
    
    Args:
        db_path: Caminho do banco de origem (usa padrão se não informado)
        
    Returns:
        True se sucesso, False se falhou
    """
    src = db_path or DB_PATH_LOCAL
    dst_dir = Path(BACKUP_REDE_DIR)
    dst = BACKUP_REDE_PATH
    
    # Verificar se o banco de origem existe
    if not Path(src).exists():
        logger.error(f"Banco de dados não encontrado: {src}")
        return False
    
    # Verificar se a pasta de rede (SMB) está acessível
    if not dst_dir.parent.exists():
        logger.warning(f"Pasta de rede não acessível: {dst_dir.parent}")
        logger.info(f"Monte o compartilhamento SMB: {SMB_URL_BACKOFFICE}")
        logger.info("  Finder > Cmd+K > Cole a URL acima > Conectar")
        return False
    
    # Criar diretório de destino se não existir
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        logger.error(f"Sem permissão para criar diretório: {dst_dir}")
        return False
    
    logger.info("Replicando banco para rede (SMB Backoffice)...")
    logger.info(f"  Origem: {src}")
    logger.info(f"  Destino: {dst}")
    logger.info(f"  (SMB: {SMB_URL_BACKOFFICE}/RETORNOS RPA - QIGGER/db.Portabilidade/portabilidade.db)")
    
    # Estratégia: criar backup local temporário, depois copiar para rede
    # (sqlite3 .backup não funciona bem com caminhos de rede diretamente)
    temp_backup = Path("/tmp/portabilidade_temp_backup.db")
    
    try:
        # 1. Criar backup local seguro
        if backup_sqlite_seguro(src, str(temp_backup)):
            # 2. Copiar para rede
            shutil.copy2(temp_backup, dst)
            
            # 3. Verificar tamanho
            size_mb = Path(dst).stat().st_size / (1024 * 1024)
            logger.info(f"✅ Replicação concluída: {size_mb:.2f} MB")
            
            # 4. Remover temporário
            temp_backup.unlink()
            return True
        else:
            # Fallback: copiar diretamente (menos seguro, mas funciona)
            logger.warning("Usando fallback: cópia direta")
            shutil.copy2(src, dst)
            size_mb = Path(dst).stat().st_size / (1024 * 1024)
            logger.info(f"✅ Replicação concluída (fallback): {size_mb:.2f} MB")
            return True
            
    except Exception as e:
        logger.error(f"❌ Falha na replicação para rede: {e}")
        # Limpar temporário se existir
        if temp_backup.exists():
            temp_backup.unlink()
        return False


def criar_backup(db_path: str = "data/portabilidade.db", replicar_rede: bool = True) -> str:
    """
    Cria backup do banco de dados (local e opcionalmente na rede)
    
    Args:
        db_path: Caminho do banco de dados
        replicar_rede: Se True, também replica para a rede
        
    Returns:
        Caminho do arquivo de backup criado
    """
    db_file = Path(db_path)
    
    if not db_file.exists():
        logger.warning(f"Banco de dados não encontrado: {db_path}")
        return None
    
    # Criar pasta de backups local
    backup_dir = Path("data/backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Nome do backup com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"portabilidade_backup_{timestamp}.db"
    backup_path = backup_dir / backup_name
    
    try:
        # Usar backup seguro do SQLite (ao invés de shutil.copy2)
        if backup_sqlite_seguro(str(db_file), str(backup_path)):
            logger.info(f"✓ Backup local criado: {backup_path}")
            
            # Verificar tamanho
            size_mb = backup_path.stat().st_size / (1024 * 1024)
            logger.info(f"  Tamanho: {size_mb:.2f} MB")
            
            # Replicar para rede se solicitado
            if replicar_rede:
                replicar_para_rede(str(db_file))
            
            return str(backup_path)
        else:
            # Fallback: usar shutil.copy2 se sqlite3 falhar
            logger.warning("Fallback: usando shutil.copy2")
            shutil.copy2(db_path, backup_path)
            
            # Copiar também arquivos WAL e SHM se existirem
            wal_file = Path(f"{db_path}-wal")
            shm_file = Path(f"{db_path}-shm")
            
            if wal_file.exists():
                shutil.copy2(wal_file, backup_dir / f"{backup_name}-wal")
            if shm_file.exists():
                shutil.copy2(shm_file, backup_dir / f"{backup_name}-shm")
            
            logger.info(f"✓ Backup criado (fallback): {backup_path}")
            
            # Verificar tamanho
            size_mb = backup_path.stat().st_size / (1024 * 1024)
            logger.info(f"  Tamanho: {size_mb:.2f} MB")
            
            return str(backup_path)
        
    except Exception as e:
        logger.error(f"Erro ao criar backup: {e}")
        return None

def listar_backups() -> list:
    """Lista todos os backups disponíveis"""
    backup_dir = Path("data/backups")
    if not backup_dir.exists():
        return []
    
    backups = []
    for backup_file in backup_dir.glob("portabilidade_backup_*.db"):
        size_mb = backup_file.stat().st_size / (1024 * 1024)
        mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
        backups.append({
            'path': str(backup_file),
            'name': backup_file.name,
            'size_mb': size_mb,
            'date': mtime
        })
    
    # Ordenar por data (mais recente primeiro)
    backups.sort(key=lambda x: x['date'], reverse=True)
    return backups

def limpar_backups_antigos(manter_ultimos: int = 10):
    """
    Remove backups antigos, mantendo apenas os N mais recentes
    
    Args:
        manter_ultimos: Número de backups a manter
    """
    backups = listar_backups()
    
    if len(backups) <= manter_ultimos:
        return
    
    # Remover backups além do limite
    for backup in backups[manter_ultimos:]:
        try:
            Path(backup['path']).unlink()
            logger.info(f"Backup antigo removido: {backup['name']}")
        except Exception as e:
            logger.warning(f"Erro ao remover backup antigo: {e}")


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Backup do banco de dados com replicação para rede"
    )
    parser.add_argument(
        '--apenas-rede',
        action='store_true',
        help='Apenas replicar para rede (sem criar backup local)'
    )
    parser.add_argument(
        '--apenas-local',
        action='store_true',
        help='Apenas criar backup local (sem replicar para rede)'
    )
    parser.add_argument(
        '--limpar',
        action='store_true',
        help='Limpar backups antigos (manter últimos 10)'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("BACKUP DO BANCO DE DADOS")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    db_path = DB_PATH_LOCAL
    
    # Modo apenas rede
    if args.apenas_rede:
        print("[1] Replicando para rede...")
        if replicar_para_rede(db_path):
            print(f"    >> Replicação concluída: {BACKUP_REDE_PATH}")
        else:
            print("    >> Erro na replicação")
            return 1
    else:
        # Criar backup local
        replicar = not args.apenas_local
        print(f"[1] Criando backup de: {db_path}")
        print(f"    >> Replicar para rede: {'Sim' if replicar else 'Não'}")
        
        backup_path = criar_backup(db_path, replicar_rede=replicar)
        
        if backup_path:
            print(f"    >> Backup local criado: {backup_path}")
        else:
            print("    >> Erro ao criar backup")
            return 1
    
    # Listar backups locais
    print("\n[2] Listando backups disponíveis...")
    backups = listar_backups()
    
    if backups:
        print(f"    >> Total de backups locais: {len(backups)}")
        print("\n    Últimos 5 backups:")
        for i, backup in enumerate(backups[:5], 1):
            print(f"      {i}. {backup['name']}")
            print(f"         Data: {backup['date'].strftime('%d/%m/%Y %H:%M:%S')}")
            print(f"         Tamanho: {backup['size_mb']:.2f} MB")
    else:
        print("    >> Nenhum backup local encontrado")
    
    # Verificar backup na rede
    print("\n[3] Verificando backup na rede...")
    rede_path = Path(BACKUP_REDE_PATH)
    if rede_path.exists():
        size_mb = rede_path.stat().st_size / (1024 * 1024)
        mtime = datetime.fromtimestamp(rede_path.stat().st_mtime)
        print(f"    >> Backup na rede: {BACKUP_REDE_PATH}")
        print(f"       Tamanho: {size_mb:.2f} MB")
        print(f"       Última atualização: {mtime.strftime('%d/%m/%Y %H:%M:%S')}")
    else:
        print(f"    >> Backup na rede não encontrado ou volume não montado")
    
    # Limpar backups antigos se solicitado
    if args.limpar:
        print("\n[4] Limpando backups antigos...")
        limpar_backups_antigos(manter_ultimos=10)
        print("    >> Limpeza concluída (mantidos últimos 10)")
    
    print("\n" + "=" * 70)
    print("BACKUP CONCLUÍDO!")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

