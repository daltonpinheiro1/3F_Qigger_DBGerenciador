"""
Script para validar arquivo de Reabertura
Valida estrutura, dados obrigatórios e links de rastreio
"""
import sys
import os
from pathlib import Path
import pandas as pd

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

print("=" * 70)
print("VALIDAÇÃO DO ARQUIVO DE REABERTURA")
print("=" * 70)

# Buscar arquivo mais recente na pasta de retornos
backoffice_path = Path("data/retornos/backoffice")
if not backoffice_path.exists():
    print(f"\nERRO: Pasta não encontrada: {backoffice_path}")
    print("Execute primeiro o processamento de arquivos para gerar os arquivos.")
    sys.exit(1)

# Buscar arquivos de reabertura
arquivos_reabertura = list(backoffice_path.glob("Reabertura_*.csv"))

if not arquivos_reabertura:
    print(f"\nAVISO: Nenhum arquivo de Reabertura encontrado em: {backoffice_path}")
    print("Execute primeiro o processamento de arquivos.")
    sys.exit(0)

# Pegar o mais recente
arquivo = max(arquivos_reabertura, key=lambda x: x.stat().st_mtime)

print(f"\nArquivo: {arquivo}")
print(f"Data de modificação: {arquivo.stat().st_mtime}")

try:
    df = pd.read_csv(arquivo, sep=';', encoding='utf-8-sig')
except Exception as e:
    print(f"\nERRO ao ler arquivo: {e}")
    sys.exit(1)

print(f"\nTotal de registros: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")

# Validar colunas obrigatórias (formato novo: agrupado por CPF)
colunas_obrigatorias = [
    'Cpf',
    'Número de acesso 1',
    'Número da ordem 1',
    'Código externo 1'
]

print("\n" + "=" * 70)
print("VALIDAÇÃO DE COLUNAS")
print("=" * 70)

colunas_faltando = []
for col in colunas_obrigatorias:
    if col not in df.columns:
        colunas_faltando.append(col)
        print(f"✗ Coluna obrigatória faltando: {col}")
    else:
        print(f"✓ Coluna presente: {col}")

if colunas_faltando:
    print(f"\nERRO: {len(colunas_faltando)} coluna(s) obrigatória(s) faltando!")
    sys.exit(1)

# Validar dados
print("\n" + "=" * 70)
print("VALIDAÇÃO DE DADOS")
print("=" * 70)

# CPF
cpfs_validos = df['Cpf'].notna() & (df['Cpf'] != '')
print(f"CPFs preenchidos: {cpfs_validos.sum()}/{len(df)} ({cpfs_validos.sum()/len(df)*100:.1f}%)")

# Código Externo 1 (primeiro código)
codigos_validos = df['Código externo 1'].notna() & (df['Código externo 1'] != '')
print(f"Códigos Externos 1 preenchidos: {codigos_validos.sum()}/{len(df)} ({codigos_validos.sum()/len(df)*100:.1f}%)")

# Número de acesso 1
acessos_validos = df['Número de acesso 1'].notna() & (df['Número de acesso 1'] != '')
print(f"Números de acesso 1 preenchidos: {acessos_validos.sum()}/{len(df)} ({acessos_validos.sum()/len(df)*100:.1f}%)")

# Número da ordem 1
ordens_validas = df['Número da ordem 1'].notna() & (df['Número da ordem 1'] != '')
print(f"Números da ordem 1 preenchidos: {ordens_validas.sum()}/{len(df)} ({ordens_validas.sum()/len(df)*100:.1f}%)")

# Verificar múltiplos registros por CPF
cpfs_com_multiplos = 0
for cpf in df['Cpf'].unique():
    if pd.notna(cpf):
        registros_cpf = df[df['Cpf'] == cpf]
        # Verificar se tem mais de um código externo preenchido
        codigos_preenchidos = sum([
            1 for i in range(1, 6) 
            if f'Código externo {i}' in df.columns and 
            registros_cpf[f'Código externo {i}'].notna().any() and 
            (registros_cpf[f'Código externo {i}'] != '').any()
        ])
        if codigos_preenchidos > 1:
            cpfs_com_multiplos += 1

print(f"\nCPFs com múltiplos códigos externos: {cpfs_com_multiplos}")

# Exemplos
print("\n" + "=" * 70)
print("EXEMPLOS DE REGISTROS")
print("=" * 70)
print("\nPrimeiros 5 registros:")
colunas_exemplo = ['Cpf', 'Número de acesso 1', 'Número da ordem 1', 'Código externo 1']
if 'Preço' in df.columns:
    colunas_exemplo.append('Preço')
print(df[colunas_exemplo].head().to_string())

print("\n" + "=" * 70)
print("VALIDAÇÃO CONCLUÍDA")
print("=" * 70)

if colunas_faltando or len(links_invalidos) > 0:
    print("\n⚠ ATENÇÃO: Foram encontrados problemas na validação!")
    sys.exit(1)
else:
    print("\n✓ Arquivo validado com sucesso!")
    sys.exit(0)

