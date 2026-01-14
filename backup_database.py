"""
Script para criar backup do banco de dados antes de reprocessamento
"""
import sys
import shutil
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/backup.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def criar_backup(db_path: str = "data/portabilidade.db") -> str:
    """
    Cria backup do banco de dados
    
    Args:
        db_path: Caminho do banco de dados
        
    Returns:
        Caminho do arquivo de backup criado
    """
    db_file = Path(db_path)
    
    if not db_file.exists():
        logger.warning(f"Banco de dados não encontrado: {db_path}")
        return None
    
    # Criar pasta de backups
    backup_dir = Path("data/backups")
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    # Nome do backup com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"portabilidade_backup_{timestamp}.db"
    backup_path = backup_dir / backup_name
    
    try:
        # Copiar arquivo
        shutil.copy2(db_path, backup_path)
        
        # Copiar também arquivos WAL e SHM se existirem
        wal_file = Path(f"{db_path}-wal")
        shm_file = Path(f"{db_path}-shm")
        
        if wal_file.exists():
            shutil.copy2(wal_file, backup_dir / f"{backup_name}-wal")
        if shm_file.exists():
            shutil.copy2(shm_file, backup_dir / f"{backup_name}-shm")
        
        logger.info(f"✓ Backup criado: {backup_path}")
        
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

def main():
    """Função principal"""
    print("=" * 70)
    print("BACKUP DO BANCO DE DADOS")
    print("=" * 70)
    print()
    
    db_path = "data/portabilidade.db"
    
    # Criar backup
    print(f"[1] Criando backup de: {db_path}")
    backup_path = criar_backup(db_path)
    
    if backup_path:
        print(f"    >> Backup criado com sucesso: {backup_path}")
    else:
        print("    >> Erro ao criar backup")
        return 1
    
    # Listar backups
    print("\n[2] Listando backups disponíveis...")
    backups = listar_backups()
    
    if backups:
        print(f"    >> Total de backups: {len(backups)}")
        print("\n    Últimos 5 backups:")
        for i, backup in enumerate(backups[:5], 1):
            print(f"      {i}. {backup['name']}")
            print(f"         Data: {backup['date'].strftime('%d/%m/%Y %H:%M:%S')}")
            print(f"         Tamanho: {backup['size_mb']:.2f} MB")
    else:
        print("    >> Nenhum backup encontrado")
    
    print("\n" + "=" * 70)
    print("BACKUP CONCLUÍDO!")
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

