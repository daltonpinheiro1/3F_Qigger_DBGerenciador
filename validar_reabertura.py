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

# Validar colunas obrigatórias
colunas_obrigatorias = [
    'CPF', 'Numero_Acesso', 'Numero_Ordem', 'Codigo_Externo',
    'Cod_Rastreio', 'Status_Bilhete', 'Status_Ordem', 'Motivo_Cancelamento'
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
cpfs_validos = df['CPF'].notna() & (df['CPF'] != '')
print(f"CPFs preenchidos: {cpfs_validos.sum()}/{len(df)} ({cpfs_validos.sum()/len(df)*100:.1f}%)")

# Código Externo
codigos_validos = df['Codigo_Externo'].notna() & (df['Codigo_Externo'] != '')
print(f"Códigos Externos preenchidos: {codigos_validos.sum()}/{len(df)} ({codigos_validos.sum()/len(df)*100:.1f}%)")

# Links de Rastreio
links_validos = df['Cod_Rastreio'].notna() & (df['Cod_Rastreio'] != '')
links_com_http = df['Cod_Rastreio'].astype(str).str.startswith('http')
print(f"Links de rastreio preenchidos: {links_validos.sum()}/{len(df)}")
print(f"Links válidos (começam com http): {links_com_http.sum()}/{len(df)}")

# Validar formato de links
links_invalidos = df[~links_com_http & links_validos]
if len(links_invalidos) > 0:
    print(f"\n⚠ {len(links_invalidos)} link(s) sem formato http:")
    print(links_invalidos[['Codigo_Externo', 'Cod_Rastreio']].head())

# Status Cancelado
status_cancelado = df['Status_Bilhete'].astype(str).str.contains('Cancelada', case=False, na=False)
print(f"\nRegistros com Status_Bilhete 'Cancelada': {status_cancelado.sum()}/{len(df)}")

# Motivo Cancelamento
motivos_preenchidos = df['Motivo_Cancelamento'].notna() & (df['Motivo_Cancelamento'] != '')
print(f"Motivos de Cancelamento preenchidos: {motivos_preenchidos.sum()}/{len(df)}")

# Validar que todos são cancelamento/reabertura
if len(df) > 0:
    todos_cancelados = status_cancelado.sum()
    print(f"Registros cancelados: {todos_cancelados}/{len(df)}")
    
    if todos_cancelados < len(df):
        nao_cancelados = df[~status_cancelado]
        print(f"\n⚠ {len(nao_cancelados)} registro(s) que não são cancelados:")
        print(nao_cancelados[['CPF', 'Codigo_Externo', 'Status_Bilhete', 'Motivo_Cancelamento']].head())

# Exemplos
print("\n" + "=" * 70)
print("EXEMPLOS DE REGISTROS")
print("=" * 70)
print("\nPrimeiros 5 registros:")
print(df[['CPF', 'Codigo_Externo', 'Status_Bilhete', 'Motivo_Cancelamento', 'Cod_Rastreio']].head().to_string())

print("\n" + "=" * 70)
print("VALIDAÇÃO CONCLUÍDA")
print("=" * 70)

if colunas_faltando or len(links_invalidos) > 0:
    print("\n⚠ ATENÇÃO: Foram encontrados problemas na validação!")
    sys.exit(1)
else:
    print("\n✓ Arquivo validado com sucesso!")
    sys.exit(0)

