"""
Script para gerar arquivo de homologação de Reabertura
Filtra registros cancelados e agrupa por CPF
"""
import sys
import os
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import sqlite3
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
from src.utils.csv_generator import CSVGenerator
from collections import defaultdict

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_reabertura.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos
DB_PATH = "data/portabilidade.db"
OUTPUT_HOMOLOGACAO = Path("data/homologacao_reabertura.xlsx")
OUTPUT_TEMP = Path("data/homologacao_reabertura_temp.xlsx")
BASE_ANALITICA_PATH = Path(r"G:\Meu Drive\3F Contact Center\base_analitica_final.csv")

def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - REABERTURA")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar registros cancelados
    print("[2] Buscando registros cancelados...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT
                cpf, numero_acesso, numero_ordem, codigo_externo,
                status_bilhete, status_ordem, operadora_doadora,
                data_portabilidade, motivo_cancelamento, motivo_recusa,
                preco_ordem
            FROM portabilidade_records
            WHERE status_bilhete = 'Portabilidade Cancelada'
               OR motivo_cancelamento IS NOT NULL
               OR motivo_cancelamento != ''
            ORDER BY data_inicial_processamento DESC
            LIMIT 1000
        """)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("\n⚠ Nenhum registro cancelado encontrado!")
        return
    
    # [3] Converter para PortabilidadeRecord
    print("[3] Processando registros...")
    from src.models.portabilidade import PortabilidadeRecord
    
    reabertura = []
    results_map = {}  # Simular results_map vazio para homologação
    
    for row in rows:
        record_dict = dict(zip(columns, row))
        
        # Criar record
        try:
            record = PortabilidadeRecord(
                cpf=record_dict.get('cpf', ''),
                numero_acesso=record_dict.get('numero_acesso', ''),
                numero_ordem=record_dict.get('numero_ordem', ''),
                codigo_externo=record_dict.get('codigo_externo', ''),
                status_bilhete=PortabilidadeStatus(record_dict['status_bilhete']) if record_dict.get('status_bilhete') else None,
                status_ordem=StatusOrdem(record_dict['status_ordem']) if record_dict.get('status_ordem') else None,
                operadora_doadora=record_dict.get('operadora_doadora'),
                data_portabilidade=datetime.fromisoformat(record_dict['data_portabilidade']) if record_dict.get('data_portabilidade') else None,
                motivo_cancelamento=record_dict.get('motivo_cancelamento'),
                motivo_recusa=record_dict.get('motivo_recusa'),
                preco_ordem=record_dict.get('preco_ordem')
            )
            reabertura.append(record)
        except Exception as e:
            logger.error(f"Erro ao criar record: {e}")
            continue
    
    print(f"    >> {len(reabertura)} registros processados")
    
    if not reabertura:
        print("\n⚠ Nenhum registro de reabertura válido encontrado!")
        return
    
    # [3.1] Carregar Base Analítica para buscar Plano
    print("[3.1] Carregando Base Analítica...")
    base_analitica_loader = None
    if BASE_ANALITICA_PATH.exists():
        try:
            # Usar o BaseAnaliticaLoader do gerar_homologacao_wpp.py
            from gerar_homologacao_wpp import BaseAnaliticaLoader
            base_analitica_loader = BaseAnaliticaLoader(str(BASE_ANALITICA_PATH))
            count = base_analitica_loader.load()
            if count > 0:
                print(f"    >> {count} registros da base analítica carregados")
        except Exception as e:
            print(f"    >> Erro ao carregar base analítica: {e}")
            logger.warning(f"Erro ao carregar Base Analítica: {e}")
    else:
        print(f"    >> Arquivo base analítica não encontrado: {BASE_ANALITICA_PATH}")
    
    # [4] Gerar arquivo de homologação
    print("[4] Gerando arquivo de homologação...")
    # Gerar em arquivo temporário primeiro para evitar problemas de permissão
    output_path = OUTPUT_TEMP
    
    if CSVGenerator.generate_reabertura_csv(
        reabertura,
        results_map,
        output_path,
        base_analitica_loader
    ):
        # Renomear para o arquivo final
        try:
            if OUTPUT_HOMOLOGACAO.exists():
                OUTPUT_HOMOLOGACAO.unlink()
            output_path.rename(OUTPUT_HOMOLOGACAO)
            output_path = OUTPUT_HOMOLOGACAO
        except Exception as e:
            logger.warning(f"Não foi possível renomear arquivo, usando temporário: {e}")
        
        print(f"    >> Arquivo salvo em: {output_path}")
        print()
        print("=" * 70)
        print("ESTATÍSTICAS DE HOMOLOGAÇÃO")
        print("=" * 70)
        print(f"  Total de registros: {len(reabertura)}")
        
        # Contar CPFs únicos
        cpfs_unicos = len(set(r.cpf for r in reabertura))
        print(f"  CPFs únicos: {cpfs_unicos}")
        print()
        print("=" * 70)
        print("HOMOLOGAÇÃO GERADA COM SUCESSO!")
        print("=" * 70)
    else:
        print("\n✗ ERRO ao gerar arquivo de homologação!")

if __name__ == "__main__":
    main()

