"""Validação final do arquivo de reabertura"""
import pandas as pd
from pathlib import Path

arquivo = Path("data/homologacao_reabertura.xlsx")

if not arquivo.exists():
    print(f"Arquivo não encontrado: {arquivo}")
    exit(1)

df = pd.read_excel(arquivo)

print("=" * 70)
print("VALIDAÇÃO FINAL - REABERTURA")
print("=" * 70)
print(f"\nTotal de registros: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")

print("\n" + "=" * 70)
print("VALIDAÇÕES ESPECÍFICAS")
print("=" * 70)

# Planos preenchidos
planos_preenchidos = df['Plano'].notna().sum()
print(f"✓ Planos preenchidos: {planos_preenchidos}/{len(df)} ({planos_preenchidos/len(df)*100:.1f}%)")

# Preços preenchidos
precos_preenchidos = df['Preço'].notna().sum()
print(f"✓ Preços preenchidos: {precos_preenchidos}/{len(df)} ({precos_preenchidos/len(df)*100:.1f}%)")

# CPFs preenchidos
cpfs_preenchidos = df['Cpf'].notna().sum()
print(f"✓ CPFs preenchidos: {cpfs_preenchidos}/{len(df)} ({cpfs_preenchidos/len(df)*100:.1f}%)")

# Códigos externos 1 preenchidos
codigos_preenchidos = df['Código externo 1'].notna().sum()
print(f"✓ Códigos externos 1 preenchidos: {codigos_preenchidos}/{len(df)} ({codigos_preenchidos/len(df)*100:.1f}%)")

# Verificar formato do Plano (deve ser apenas valor, ex: "31,99")
planos_com_valor = 0
for plano in df['Plano'].dropna():
    plano_str = str(plano).strip()
    # Verificar se contém apenas números e vírgula/ponto (formato de preço)
    if any(c.isdigit() for c in plano_str) and (',' in plano_str or '.' in plano_str):
        planos_com_valor += 1

print(f"✓ Planos com formato de valor: {planos_com_valor}/{planos_preenchidos}")

print("\n" + "=" * 70)
print("EXEMPLOS")
print("=" * 70)
print("\nPrimeiras 10 linhas:")
print(df[['Cpf', 'Plano', 'Preço', 'Código externo 1']].head(10).to_string())

print("\n" + "=" * 70)
print("RESUMO")
print("=" * 70)
if planos_preenchidos > 0 and cpfs_preenchidos == len(df) and codigos_preenchidos == len(df):
    print("✓ TODAS AS VALIDAÇÕES PASSARAM!")
    print("✓ Arquivo pronto para homologação!")
else:
    print("⚠ ALGUMAS VALIDAÇÕES FALHARAM!")
    if planos_preenchidos == 0:
        print("  - Nenhum plano preenchido")
    if cpfs_preenchidos != len(df):
        print(f"  - CPFs: {len(df) - cpfs_preenchidos} não preenchidos")
    if codigos_preenchidos != len(df):
        print(f"  - Códigos externos: {len(df) - codigos_preenchidos} não preenchidos")

print("=" * 70)

