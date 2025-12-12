"""
Utilitários para configuração do console no Windows
"""
import sys
import os


def setup_windows_console():
    """Configura o console do Windows para suportar UTF-8"""
    if sys.platform == 'win32':
        try:
            # Tentar configurar o código de página do console para UTF-8
            os.system('chcp 65001 >nul 2>&1')
            
            # Configurar variáveis de ambiente
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            if hasattr(sys.stderr, 'reconfigure'):
                sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            # Se falhar, continuar normalmente
            pass


def safe_print(text: str):
    """
    Imprime texto de forma segura no console Windows
    
    Args:
        text: Texto a ser impresso
    """
    try:
        print(text)
    except UnicodeEncodeError:
        # Fallback: substituir caracteres problemáticos
        safe_text = text.encode('ascii', errors='replace').decode('ascii')
        print(safe_text)

