"""Validação final do arquivo de aprovisionamentos"""
import pandas as pd
from pathlib import Path

arquivo = Path("data/homologacao_aprovisionamentos.csv")

if not arquivo.exists():
    print(f"Arquivo não encontrado: {arquivo}")
    exit(1)

df = pd.read_csv(arquivo, sep=';', encoding='utf-8-sig')

print("=" * 70)
print("VALIDAÇÃO FINAL - APROVISIONAMENTOS")
print("=" * 70)
print(f"\nTotal de registros: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")

print("\n" + "=" * 70)
print("VALIDAÇÕES ESPECÍFICAS")
print("=" * 70)

# Último bilhete sempre Sim
ultimo_bilhete_sim = (df['Último bilhete de portabilidade?'] == 'Sim').all()
print(f"✓ Último bilhete sempre Sim: {ultimo_bilhete_sim} ({df['Último bilhete de portabilidade?'].value_counts().get('Sim', 0)}/{len(df)})")

# Status da entrega
status_entrega_preenchidos = df['Status da entrega'].notna().sum()
print(f"✓ Status da entrega preenchidos: {status_entrega_preenchidos}/{len(df)} ({status_entrega_preenchidos/len(df)*100:.1f}%)")

# Data da entrega
data_entrega_preenchidas = df['Data da entrega'].notna().sum()
print(f"✓ Data da entrega preenchidas: {data_entrega_preenchidas}/{len(df)} ({data_entrega_preenchidas/len(df)*100:.1f}%)")

# Status da ordem
status_ordem_validos = df['Status da ordem'].astype(str).str.contains('Em Aprovisionamento|Erro no Aprovisionamento', case=False, na=False).sum()
print(f"✓ Status da ordem válidos: {status_ordem_validos}/{len(df)} ({status_ordem_validos/len(df)*100:.1f}%)")

# Motivos excluídos
motivos_excluir = [
    'Rejeição do Cliente via SMS',
    'CPF Inválido',
    'Portabilidade de Número Vago',
    'Tipo de cliente inválido'
]
total_excluidos = 0
for motivo in motivos_excluir:
    recusa = df['Motivo da recusa'].astype(str).str.contains(motivo, case=False, na=False).sum()
    cancel = df['Motivo do cancelamento'].astype(str).str.contains(motivo, case=False, na=False).sum()
    total_excluidos += (recusa + cancel)

print(f"✓ Motivos de exclusão: {total_excluidos} encontrados (deve ser 0)")

print("\n" + "=" * 70)
print("RESUMO")
print("=" * 70)
if ultimo_bilhete_sim and status_ordem_validos == len(df) and total_excluidos == 0:
    print("✓ TODAS AS VALIDAÇÕES PASSARAM!")
    print("✓ Arquivo pronto para homologação!")
else:
    print("✗ ALGUMAS VALIDAÇÕES FALHARAM!")
    if not ultimo_bilhete_sim:
        print("  - Último bilhete não está sempre Sim")
    if status_ordem_validos != len(df):
        print(f"  - Status da ordem: {len(df) - status_ordem_validos} inválidos")
    if total_excluidos > 0:
        print(f"  - Motivos de exclusão: {total_excluidos} encontrados")

print("=" * 70)

