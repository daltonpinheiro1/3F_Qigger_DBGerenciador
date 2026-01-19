"""
Script para gerar arquivo WPP apenas com IDs específicos da lista ids_forcar_wpp.txt
Ignora regras de crivo_vendas e template - força inclusão dos IDs listados.
"""
import sys
from pathlib import Path
from datetime import datetime
import csv

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/wpp_ids_forcados.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager

# Caminhos
try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "data/portabilidade.db"

IDS_FORCAR_PATH = Path(__file__).parent / "data" / "ids_forcar_wpp.txt"
OUTPUT_PATH = Path(__file__).parent / "data" / "homologacao_wpp_forcados.csv"


def carregar_ids_forcar() -> set:
    """Carrega IDs do arquivo txt."""
    ids = set()
    if not IDS_FORCAR_PATH.exists():
        logger.error(f"Arquivo não encontrado: {IDS_FORCAR_PATH}")
        return ids
    
    with open(IDS_FORCAR_PATH, 'r', encoding='utf-8') as f:
        for linha in f:
            linha = linha.strip()
            if not linha or linha.startswith('#'):
                continue
            id_limpo = ''.join(filter(str.isdigit, linha))
            if id_limpo:
                ids.add(id_limpo)
    
    return ids


def normalizar_telefone(telefone: str) -> str:
    """Normaliza telefone para 11 dígitos."""
    if not telefone:
        return ""
    apenas_digitos = ''.join(filter(str.isdigit, str(telefone)))
    if len(apenas_digitos) == 11:
        return apenas_digitos
    elif len(apenas_digitos) == 10:
        return f"9{apenas_digitos}"  # Adiciona 9 se faltar
    return apenas_digitos


def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO WPP - IDs FORÇADOS")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # 1. Carregar IDs
    print("[1] Carregando IDs do arquivo...")
    ids_forcar = carregar_ids_forcar()
    if not ids_forcar:
        print("    >> Nenhum ID encontrado. Abortando.")
        return
    print(f"    >> {len(ids_forcar)} IDs carregados")
    
    # 2. Conectar ao banco
    print("[2] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # 3. Buscar dados dos IDs
    print("[3] Buscando dados dos IDs...")
    
    # Construir query com os IDs
    placeholders = ','.join([f"'{id}'" for id in ids_forcar])
    
    query = f"""
    SELECT DISTINCT
        COALESCE(bc.proposta_isize, bc.codigo_externo) AS proposta_isize,
        bc.cpf,
        bc.cliente_nome,
        bc.telefone_portado,
        bc.endereco,
        bc.numero,
        bc.complemento,
        bc.bairro,
        bc.cidade,
        bc.uf,
        bc.cep,
        bc.ponto_referencia,
        bc.data_venda,
        bc.crivo_vendas,
        COALESCE(pr.template, '1') AS template,
        pr.o_que_aconteceu,
        pr.acao_a_realizar
    FROM base_coverte_prop bc
    LEFT JOIN portabilidade_records pr ON (
        TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
        TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
    )
    WHERE bc.proposta_isize IN ({placeholders})
       OR bc.codigo_externo IN ({placeholders})
    ORDER BY bc.data_venda DESC
    """
    
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("    >> Nenhum registro encontrado. Verifique se os IDs existem no banco.")
        return
    
    # 4. Preparar dados para CSV
    print("[4] Preparando arquivo CSV...")
    
    dados_csv = []
    ids_encontrados = set()
    
    for row in rows:
        row_dict = dict(zip(columns, row))
        
        proposta = str(row_dict.get('proposta_isize', '') or '').strip()
        if proposta in ids_encontrados:
            continue  # Evitar duplicados
        ids_encontrados.add(proposta)
        
        # Normalizar telefone
        telefone = normalizar_telefone(row_dict.get('telefone_portado', ''))
        
        # Formatar data
        data_venda = row_dict.get('data_venda', '')
        if data_venda:
            try:
                if isinstance(data_venda, str) and len(data_venda) >= 10:
                    if '-' in data_venda:
                        # Formato YYYY-MM-DD
                        partes = data_venda[:10].split('-')
                        data_venda = f"{partes[2]}/{partes[1]}/{partes[0]}"
            except:
                pass
        
        dados_csv.append({
            'Proposta_iSize': proposta,
            'Cpf': str(row_dict.get('cpf', '') or '').strip(),
            'NomeCliente': str(row_dict.get('cliente_nome', '') or '').strip(),
            'Telefone_Contato': telefone,
            'Endereco': str(row_dict.get('endereco', '') or '').strip(),
            'Numero': str(row_dict.get('numero', '') or '').strip(),
            'Complemento': str(row_dict.get('complemento', '') or '').strip(),
            'Bairro': str(row_dict.get('bairro', '') or '').strip(),
            'Cidade': str(row_dict.get('cidade', '') or '').strip(),
            'UF': str(row_dict.get('uf', '') or '').strip(),
            'Cep': str(row_dict.get('cep', '') or '').strip(),
            'Ponto_Referencia': str(row_dict.get('ponto_referencia', '') or '').strip(),
            'Cod_Rastreio': '',  # Será preenchido depois
            'Data_Venda': data_venda or '',
            'Tipo_Comunicacao': '1',  # Forçar tipo 1
            'Tentativas': '0',
            'Total_Classificacoes': '1',
            'Houve_Reclassificacao': 'NAO',
            'Status_Disparo': 'FALSE',
            'DataHora_Disparo': '',
            'Template_Triggers': str(row_dict.get('template', '1') or '1'),
            'O_Que_Aconteceu': str(row_dict.get('o_que_aconteceu', '') or ''),
            'Acao_Realizar': str(row_dict.get('acao_a_realizar', '') or ''),
            'Crivo_Vendas': str(row_dict.get('crivo_vendas', '') or ''),
        })
    
    # 5. Salvar CSV
    print("[5] Salvando arquivo...")
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    fieldnames = list(dados_csv[0].keys()) if dados_csv else []
    
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(dados_csv)
    
    print(f"    >> Arquivo salvo: {OUTPUT_PATH}")
    
    # 6. Estatísticas
    print()
    print("=" * 70)
    print("ESTATÍSTICAS")
    print("=" * 70)
    print(f"  IDs solicitados: {len(ids_forcar)}")
    print(f"  IDs encontrados: {len(ids_encontrados)}")
    print(f"  IDs não encontrados: {len(ids_forcar - ids_encontrados)}")
    
    # Mostrar alguns IDs não encontrados
    nao_encontrados = ids_forcar - ids_encontrados
    if nao_encontrados:
        print()
        print("  Primeiros 10 IDs não encontrados:")
        for i, id in enumerate(list(nao_encontrados)[:10]):
            print(f"    - {id}")
    
    print()
    print("=" * 70)
    print("CONCLUÍDO!")
    print("=" * 70)


if __name__ == "__main__":
    main()
