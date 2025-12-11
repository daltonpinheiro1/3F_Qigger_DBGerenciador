"""
Exemplos de uso do 3F Qigger DB Gerenciador
"""
import sys
from pathlib import Path

# Adicionar o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.models.portabilidade import (
    PortabilidadeRecord,
    PortabilidadeStatus,
    StatusOrdem
)
from src.utils import CSVParser


def exemplo_1_processamento_simples():
    """Exemplo 1: Processamento simples de um registro"""
    print("=" * 60)
    print("Exemplo 1: Processamento Simples de um Registro")
    print("=" * 60)
    
    # Criar engine (sem banco de dados para exemplo simples)
    engine = QiggerDecisionEngine()
    
    # Criar registro de exemplo
    record = PortabilidadeRecord(
        cpf="12345678901",
        numero_acesso="11987654321",
        numero_ordem="1-1234567890123",
        codigo_externo="250001234",
        status_bilhete=PortabilidadeStatus.CANCELADA,
        operadora_doadora="VIVO",
        data_portabilidade=datetime(2025, 12, 10, 14, 0, 0),
        motivo_cancelamento="Cancelamento pelo Cliente",
        status_ordem=StatusOrdem.CONCLUIDO,
        preco_ordem="R$29,99",
        registro_valido=True,
        numero_acesso_valido=True
    )
    
    # Processar
    results = engine.process_record(record)
    
    # Exibir resultados
    print(f"\nRegistro processado:")
    print(f"  CPF: {record.cpf}")
    print(f"  Número de acesso: {record.numero_acesso}")
    print(f"  Status: {record.status_bilhete.value if record.status_bilhete else 'N/A'}")
    
    print(f"\n{len(results)} regra(s) aplicada(s):\n")
    for i, result in enumerate(results, 1):
        print(f"{i}. {result.rule_name}")
        print(f"   Decisão: {result.decision}")
        print(f"   Ação: {result.action}")
        print(f"   Prioridade: {result.priority}")
        print()


def exemplo_2_com_banco_dados():
    """Exemplo 2: Processamento com banco de dados"""
    print("=" * 60)
    print("Exemplo 2: Processamento com Banco de Dados")
    print("=" * 60)
    
    # Criar banco de dados e engine
    db_manager = DatabaseManager("data/exemplo.db")
    engine = QiggerDecisionEngine(db_manager)
    
    # Criar múltiplos registros
    records = [
        PortabilidadeRecord(
            cpf="11111111111",
            numero_acesso="11911111111",
            numero_ordem="1-1111111111111",
            codigo_externo="250001111",
            status_bilhete=PortabilidadeStatus.PENDENTE,
            ultimo_bilhete=True
        ),
        PortabilidadeRecord(
            cpf="22222222222",
            numero_acesso="11922222222",
            numero_ordem="1-2222222222222",
            codigo_externo="250002222",
            status_bilhete=PortabilidadeStatus.CONFLITO,
            operadora_doadora="CLARO"
        ),
        PortabilidadeRecord(
            cpf="33333333333",
            numero_acesso="11933333333",
            numero_ordem="1-3333333333333",
            codigo_externo="250003333",
            motivo_nao_consultado="Cliente sem cadastro"
        ),
    ]
    
    # Processar cada registro
    for i, record in enumerate(records, 1):
        print(f"\nProcessando registro {i}...")
        results = engine.process_record(record)
        print(f"  → {len(results)} regra(s) aplicada(s)")
        
        # Buscar do banco
        db_record = db_manager.get_record(
            record.cpf,
            record.numero_acesso,
            record.numero_ordem
        )
        if db_record:
            print(f"  → Registro salvo no banco (ID: {db_record.get('id', 'N/A')})")
    
    # Listar todos os registros
    print(f"\n\nTotal de registros no banco: {len(db_manager.get_all_records())}")


def exemplo_3_validacoes():
    """Exemplo 3: Teste de validações"""
    print("=" * 60)
    print("Exemplo 3: Teste de Validações")
    print("=" * 60)
    
    engine = QiggerDecisionEngine()
    
    # Teste 1: CPF inválido
    print("\n1. Teste: CPF inválido (tamanho incorreto)")
    record1 = PortabilidadeRecord(
        cpf="123456789",  # Muito curto
        numero_acesso="11987654321",
        numero_ordem="1-1234567890123",
        codigo_externo="250001234"
    )
    results1 = engine.process_record(record1)
    for result in results1:
        if "validar_cpf" in result.rule_name:
            print(f"   ✓ {result.decision}: {result.details}")
    
    # Teste 2: Número de acesso inválido
    print("\n2. Teste: Número de acesso inválido (muito curto)")
    record2 = PortabilidadeRecord(
        cpf="12345678901",
        numero_acesso="1198765",  # Muito curto
        numero_ordem="1-1234567890123",
        codigo_externo="250001234"
    )
    results2 = engine.process_record(record2)
    for result in results2:
        if "validar_numero_acesso" in result.rule_name:
            print(f"   ✓ {result.decision}: {result.details}")
    
    # Teste 3: Campos obrigatórios faltando
    print("\n3. Teste: Campos obrigatórios faltando")
    record3 = PortabilidadeRecord(
        cpf="",  # Vazio
        numero_acesso="11987654321",
        numero_ordem="1-1234567890123",
        codigo_externo="250001234"
    )
    results3 = engine.process_record(record3)
    for result in results3:
        if "validar_campos_obrigatorios" in result.rule_name:
            print(f"   ✓ {result.decision}: {result.details}")


def exemplo_4_todas_as_regras():
    """Exemplo 4: Demonstrar todas as 23 regras"""
    print("=" * 60)
    print("Exemplo 4: Demonstração de Todas as 23 Regras")
    print("=" * 60)
    
    engine = QiggerDecisionEngine()
    
    print(f"\nTotal de regras registradas: {len(engine.rules_registry)}\n")
    
    print("Lista de regras:")
    for i, rule_name in enumerate(engine.rules_registry.keys(), 1):
        print(f"  {i:2d}. {rule_name}")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("3F Qigger DB Gerenciador - Exemplos de Uso")
    print("=" * 60 + "\n")
    
    # Executar exemplos
    exemplo_1_processamento_simples()
    print("\n")
    
    exemplo_2_com_banco_dados()
    print("\n")
    
    exemplo_3_validacoes()
    print("\n")
    
    exemplo_4_todas_as_regras()
    print("\n" + "=" * 60)
    print("Exemplos concluídos!")
    print("=" * 60)

