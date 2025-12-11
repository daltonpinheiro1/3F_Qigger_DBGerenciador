"""
Script de teste rápido para verificar se tudo está funcionando
"""
import sys
from pathlib import Path

print("=" * 60)
print("3F Qigger DB Gerenciador - Teste Rápido")
print("=" * 60)
print()

# Teste 1: Importações
print("1. Testando importações...")
try:
    from src.engine import QiggerDecisionEngine
    from src.database import DatabaseManager
    from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus
    from src.utils import CSVParser
    from src.monitor import FolderMonitor
    print("   ✓ Todas as importações funcionando")
except Exception as e:
    print(f"   ✗ Erro nas importações: {e}")
    sys.exit(1)

# Teste 2: Criar engine
print("\n2. Testando criação da engine...")
try:
    engine = QiggerDecisionEngine()
    assert len(engine.rules_registry) == 23
    print(f"   ✓ Engine criada com {len(engine.rules_registry)} regras")
except Exception as e:
    print(f"   ✗ Erro ao criar engine: {e}")
    sys.exit(1)

# Teste 3: Criar banco de dados
print("\n3. Testando banco de dados...")
try:
    db_manager = DatabaseManager("data/teste_rapido.db")
    print("   ✓ Banco de dados criado com sucesso")
except Exception as e:
    print(f"   ✗ Erro ao criar banco: {e}")
    sys.exit(1)

# Teste 4: Processar registro
print("\n4. Testando processamento de registro...")
try:
    from datetime import datetime
    record = PortabilidadeRecord(
        cpf="12345678901",
        numero_acesso="11987654321",
        numero_ordem="1-1234567890123",
        codigo_externo="250001234",
        status_bilhete=PortabilidadeStatus.CANCELADA
    )
    
    results = engine.process_record(record)
    print(f"   ✓ Registro processado: {len(results)} regra(s) aplicada(s)")
except Exception as e:
    print(f"   ✗ Erro ao processar registro: {e}")
    sys.exit(1)

# Teste 5: Verificar diretórios
print("\n5. Verificando diretórios...")
dirs = ['data', 'logs']
for d in dirs:
    if Path(d).exists():
        print(f"   ✓ Diretório '{d}' existe")
    else:
        print(f"   ⚠ Diretório '{d}' não existe (será criado automaticamente)")

print("\n" + "=" * 60)
print("✓ Todos os testes passaram! Sistema pronto para uso.")
print("=" * 60)
print("\nPróximos passos:")
print("  1. Processar CSV: py main.py --csv arquivo.csv")
print("  2. Monitorar pasta: py main.py --watch pasta/")
print("  3. Ver exemplos: py examples/exemplo_uso.py")
print("  4. Executar testes: py -m pytest tests/ -v")

