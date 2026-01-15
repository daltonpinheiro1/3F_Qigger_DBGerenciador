"""
Script para verificar se todas as dependências estão instaladas corretamente
"""
import sys
from pathlib import Path

def verificar_dependencia(nome, import_name=None):
    """Verifica se uma dependência está instalada"""
    if import_name is None:
        import_name = nome
    
    try:
        __import__(import_name)
        print(f"  ✓ {nome}")
        return True
    except ImportError:
        print(f"  ✗ {nome} - NÃO INSTALADO")
        return False

def main():
    """Verifica todas as dependências"""
    print("=" * 60)
    print("Verificação de Dependências - 3F Qigger DB Gerenciador")
    print("=" * 60)
    print()
    
    dependencias_obrigatorias = [
        ("python-dateutil", "dateutil"),
        ("watchdog", "watchdog"),
    ]
    
    dependencias_opcionais = [
        ("pytest", "pytest"),
        ("pytest-cov", "pytest_cov"),
        ("pytest-mock", "pytest_mock"),
    ]
    
    print("Verificando dependências OBRIGATÓRIAS:")
    print()
    todas_ok = True
    for nome, import_name in dependencias_obrigatorias:
        if not verificar_dependencia(nome, import_name):
            todas_ok = False
    
    print()
    print("Verificando dependências OPCIONAIS (para testes):")
    print()
    for nome, import_name in dependencias_opcionais:
        verificar_dependencia(nome, import_name)
    
    print()
    print("Verificando módulos do projeto:")
    print()
    
    # Verificar módulos internos
    modulos_internos = [
        "src.engine",
        "src.database",
        "src.models",
        "src.utils",
        "src.monitor",
    ]
    
    for modulo in modulos_internos:
        try:
            __import__(modulo)
            print(f"  ✓ {modulo}")
        except ImportError as e:
            print(f"  ✗ {modulo} - ERRO: {e}")
            todas_ok = False
    
    print()
    print("Verificando estrutura de pastas:")
    print()
    
    pastas_necessarias = ["data", "logs", "src"]
    for pasta in pastas_necessarias:
        if Path(pasta).exists():
            print(f"  ✓ {pasta}/")
        else:
            print(f"  ✗ {pasta}/ - PASTA NÃO ENCONTRADA")
            try:
                Path(pasta).mkdir(parents=True, exist_ok=True)
                print(f"    → Pasta criada")
            except Exception as e:
                print(f"    → ERRO ao criar: {e}")
                todas_ok = False
    
    print()
    print("=" * 60)
    if todas_ok:
        print("✓ Tudo OK! Sistema pronto para uso.")
        return 0
    else:
        print("✗ Alguns problemas foram encontrados.")
        print("Execute: instalar_dependencias.bat")
        return 1

if __name__ == "__main__":
    sys.exit(main())

