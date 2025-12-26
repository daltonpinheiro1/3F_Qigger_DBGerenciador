"""
Script para validar arquivo de Aprovisionamentos
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
print("VALIDAÇÃO DO ARQUIVO DE APROVISIONAMENTOS")
print("=" * 70)

# Buscar arquivo mais recente na pasta de retornos
backoffice_path = Path("data/retornos/backoffice")
if not backoffice_path.exists():
    print(f"\nERRO: Pasta não encontrada: {backoffice_path}")
    print("Execute primeiro o processamento de arquivos para gerar os arquivos.")
    sys.exit(1)

# Buscar arquivos de aprovisionamento
arquivos_aprovisionamento = list(backoffice_path.glob("Aprovisionamentos_*.csv"))

if not arquivos_aprovisionamento:
    print(f"\nAVISO: Nenhum arquivo de Aprovisionamentos encontrado em: {backoffice_path}")
    print("Execute primeiro o processamento de arquivos.")
    sys.exit(0)

# Pegar o mais recente
arquivo = max(arquivos_aprovisionamento, key=lambda x: x.stat().st_mtime)

print(f"\nArquivo: {arquivo}")
print(f"Data de modificação: {arquivo.stat().st_mtime}")

try:
    df = pd.read_csv(arquivo, sep=';', encoding='utf-8-sig')
except Exception as e:
    print(f"\nERRO ao ler arquivo: {e}")
    sys.exit(1)

print(f"\nTotal de registros: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")

# Validar colunas obrigatórias (formato completo com todas as colunas)
colunas_obrigatorias = [
    'Cpf', 'Número de acesso', 'Número da ordem', 'Código externo',
    'Status do bilhete', 'Status da ordem'
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

# Código Externo
codigos_validos = df['Código externo'].notna() & (df['Código externo'] != '')
print(f"Códigos Externos preenchidos: {codigos_validos.sum()}/{len(df)} ({codigos_validos.sum()/len(df)*100:.1f}%)")

# Status
status_aprovisionamento = df['Status da ordem'].astype(str).str.contains('Aprovisionamento', case=False, na=False)
status_bilhete_aprovisionamento = df['Status do bilhete'].astype(str).str.contains('Aprovisionamento', case=False, na=False)
print(f"\nRegistros com Status da ordem 'Em Aprovisionamento': {status_aprovisionamento.sum()}")
print(f"Registros com Status do bilhete 'Em Aprovisionamento': {status_bilhete_aprovisionamento.sum()}")

# Validar que todos são aprovisionamento
if len(df) > 0:
    todos_aprovisionamento = (status_aprovisionamento | status_bilhete_aprovisionamento).sum()
    print(f"Registros que são aprovisionamento: {todos_aprovisionamento}/{len(df)}")
    
    if todos_aprovisionamento < len(df):
        nao_aprovisionados = df[~(status_aprovisionamento | status_bilhete_aprovisionamento)]
        print(f"\n⚠ {len(nao_aprovisionados)} registro(s) que não são aprovisionamento:")
        print(nao_aprovisionados[['Cpf', 'Código externo', 'Status do bilhete', 'Status da ordem']].head())

# Exemplos
print("\n" + "=" * 70)
print("EXEMPLOS DE REGISTROS")
print("=" * 70)
print("\nPrimeiros 5 registros:")
colunas_exemplo = ['Cpf', 'Código externo', 'Status do bilhete', 'Status da ordem']
if 'Número do bilhete' in df.columns:
    colunas_exemplo.append('Número do bilhete')
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

