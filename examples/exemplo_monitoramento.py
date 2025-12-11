"""
Exemplo de uso do monitoramento de pasta com watchdog
"""
import sys
import time
from pathlib import Path

# Adicionar o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.monitor import FolderMonitor
from src.database import DatabaseManager


def exemplo_monitoramento_simples():
    """Exemplo 1: Monitoramento simples de uma pasta"""
    print("=" * 60)
    print("Exemplo 1: Monitoramento Simples de Pasta")
    print("=" * 60)
    
    # Criar pasta temporária para exemplo
    watch_folder = Path("data/watch_example")
    watch_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"\nMonitorando pasta: {watch_folder}")
    print("Coloque arquivos CSV nesta pasta para processamento automático")
    print("Pressione Ctrl+C para parar\n")
    
    try:
        with FolderMonitor(
            watch_folder=str(watch_folder),
            db_path="data/monitor_example.db"
        ) as monitor:
            # Manter rodando
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário")


def exemplo_monitoramento_com_pastas():
    """Exemplo 2: Monitoramento com pastas de processados e erros"""
    print("=" * 60)
    print("Exemplo 2: Monitoramento com Pastas Organizadas")
    print("=" * 60)
    
    # Criar estrutura de pastas
    base_folder = Path("data/monitor_organized")
    watch_folder = base_folder / "entrada"
    processed_folder = base_folder / "processados"
    error_folder = base_folder / "erros"
    
    watch_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"\nEstrutura de pastas:")
    print(f"  Entrada: {watch_folder}")
    print(f"  Processados: {processed_folder}")
    print(f"  Erros: {error_folder}")
    print("\nColoque arquivos CSV na pasta 'entrada'")
    print("Arquivos processados serão movidos para 'processados'")
    print("Arquivos com erro serão movidos para 'erros'")
    print("Pressione Ctrl+C para parar\n")
    
    try:
        with FolderMonitor(
            watch_folder=str(watch_folder),
            db_path="data/monitor_organized.db",
            processed_folder=str(processed_folder),
            error_folder=str(error_folder)
        ) as monitor:
            # Manter rodando
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário")


def exemplo_monitoramento_nao_recursivo():
    """Exemplo 3: Monitoramento não recursivo (apenas pasta principal)"""
    print("=" * 60)
    print("Exemplo 3: Monitoramento Não Recursivo")
    print("=" * 60)
    
    watch_folder = Path("data/watch_non_recursive")
    watch_folder.mkdir(parents=True, exist_ok=True)
    
    print(f"\nMonitorando apenas a pasta principal: {watch_folder}")
    print("Subpastas NÃO serão monitoradas")
    print("Pressione Ctrl+C para parar\n")
    
    try:
        with FolderMonitor(
            watch_folder=str(watch_folder),
            db_path="data/monitor_non_recursive.db",
            recursive=False
        ) as monitor:
            # Manter rodando
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Exemplos de monitoramento de pasta"
    )
    
    parser.add_argument(
        '--exemplo',
        type=int,
        choices=[1, 2, 3],
        default=1,
        help='Número do exemplo a executar (1-3)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print("3F Qigger DB Gerenciador - Exemplos de Monitoramento")
    print("=" * 60 + "\n")
    
    if args.exemplo == 1:
        exemplo_monitoramento_simples()
    elif args.exemplo == 2:
        exemplo_monitoramento_com_pastas()
    elif args.exemplo == 3:
        exemplo_monitoramento_nao_recursivo()
    
    print("\n" + "=" * 60)
    print("Exemplo concluído!")
    print("=" * 60)

