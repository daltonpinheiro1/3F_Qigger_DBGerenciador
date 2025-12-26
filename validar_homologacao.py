"""
Script para validar o arquivo de homologação gerado
"""
import pandas as pd
from pathlib import Path

arquivo = Path("data/homologacao_wpp.csv")

print("=" * 70)
print("VALIDAÇÃO DO ARQUIVO DE HOMOLOGAÇÃO")
print("=" * 70)

df = pd.read_csv(arquivo, sep=';', encoding='utf-8-sig')

print(f"\nTotal de registros: {len(df)}")
print(f"Total de colunas: {len(df.columns)}")

print("\n=== ORDEM DAS COLUNAS ===")
print(list(df.columns))

print("\n=== PRIMEIRAS 3 LINHAS ===")
colunas_principais = ['Proposta_iSize', 'Cpf', 'NomeCliente', 'Telefone_Contato', 'Cep', 
                      'Data_Venda', 'Tipo_Comunicacao', 'Status_Disparo', 'DataHora_Disparo',
                      'Template_Triggers', 'O_Que_Aconteceu', 'Acao_Realizar']
print(df[colunas_principais].head(3).to_string())

print("\n=== VALIDAÇÕES ===")

# Converter para string para validação
df['Telefone_Contato'] = df['Telefone_Contato'].astype(str)
df['Cep'] = df['Cep'].astype(str)
df['Status_Disparo'] = df['Status_Disparo'].astype(str)
df['DataHora_Disparo'] = df['DataHora_Disparo'].fillna('').astype(str)
df['Template_Triggers'] = df['Template_Triggers'].fillna('').astype(str)
df['Tipo_Comunicacao'] = df['Tipo_Comunicacao'].astype(str)

# Validar telefones
telefones_validos = df['Telefone_Contato'].str.len() == 11
telefones_10_digitos = df['Telefone_Contato'].str.len() == 10
telefones_vazios = (df['Telefone_Contato'] == '') | (df['Telefone_Contato'] == 'nan')
print(f"Telefones com 11 dígitos: {telefones_validos.sum()}/{len(df)}")
print(f"Telefones com 10 dígitos: {telefones_10_digitos.sum()}/{len(df)}")
print(f"Telefones vazios: {telefones_vazios.sum()}/{len(df)}")

# Validar CEPs
ceps_validos = df['Cep'].str.len() == 8
ceps_vazios = (df['Cep'] == '') | (df['Cep'] == 'nan')
print(f"CEPs com 8 dígitos: {ceps_validos.sum()}/{len(df)}")
print(f"CEPs vazios: {ceps_vazios.sum()}/{len(df)}")

# Validar Status_Disparo
status_false = (df['Status_Disparo'].str.upper() == 'FALSE').all()
print(f"Status_Disparo sempre FALSE: {status_false}")

# Validar DataHora_Disparo
datahora_vazio = (df['DataHora_Disparo'] == '').all()
print(f"DataHora_Disparo sempre vazio: {datahora_vazio}")

# Validar Tipo_Comunicacao
template_upper = df['Template_Triggers'].str.upper()
tipo_com_em_criacao = df[template_upper.isin(['EM CRIAÇÃO', 'EM CRIACAO', 'EM_CRIACAO'])]
tipo_com_substituido = tipo_com_em_criacao[tipo_com_em_criacao['Tipo_Comunicacao'] == '1']
print(f"Tipo_Comunicacao com EM CRIAÇÃO: {len(tipo_com_em_criacao)}")
print(f"Tipo_Comunicacao substituído para 1: {len(tipo_com_substituido)}/{len(tipo_com_em_criacao)}")

# Validar Data_Venda formato
df['Data_Venda'] = df['Data_Venda'].fillna('').astype(str)
datas_com_formato = df['Data_Venda'].str.match(r'\d{2}/\d{2}/\d{4}', na=False)
print(f"Data_Venda no formato DD/MM/AAAA: {datas_com_formato.sum()}/{len(df)}")

print("\n=== EXEMPLOS DE TELEFONES NÃO NORMALIZADOS ===")
telefones_nao_normalizados = df[~telefones_validos & ~telefones_vazios]
if len(telefones_nao_normalizados) > 0:
    print(telefones_nao_normalizados[['Proposta_iSize', 'Telefone_Contato']].head(10).to_string())
else:
    print("Todos os telefones estão normalizados!")

print("\n=== EXEMPLOS DE CEPs NÃO NORMALIZADOS ===")
ceps_nao_normalizados = df[~ceps_validos & ~ceps_vazios]
if len(ceps_nao_normalizados) > 0:
    print(ceps_nao_normalizados[['Proposta_iSize', 'Cep']].head(10).to_string())
else:
    print("Todos os CEPs estão normalizados!")

print("\n" + "=" * 70)
print("VALIDAÇÃO CONCLUÍDA")
print("=" * 70)

