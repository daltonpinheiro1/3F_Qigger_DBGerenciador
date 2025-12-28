"""
Script para gerar arquivo de homologação de Aprovisionamentos
Filtra registros em aprovisionamento E entregue (status 6 ou data_entrega)
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
from src.utils.objects_loader import ObjectsLoader
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
from src.utils.csv_generator import CSVGenerator
from collections import defaultdict
import pandas as pd

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_aprovisionamentos.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos
DB_PATH = "data/portabilidade.db"
OUTPUT_HOMOLOGACAO = Path("data/homologacao_aprovisionamentos.csv")
OUTPUT_TEMP = Path("data/homologacao_aprovisionamentos_temp.csv")
OBJECTS_PATH = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
BASE_ANALITICA_PATH = Path(r"G:\Meu Drive\3F Contact Center\base_analitica_final.csv")

# Importar BaseAnaliticaLoader
from gerar_homologacao_wpp import BaseAnaliticaLoader

def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - APROVISIONAMENTOS")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar registros em aprovisionamento
    print("[2] Buscando registros em aprovisionamento...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT
                cpf, numero_acesso, numero_ordem, codigo_externo,
                status_bilhete, status_ordem, operadora_doadora,
                data_portabilidade, motivo_recusa, motivo_cancelamento,
                preco_ordem, numero_bilhete, numero_temporario,
                bilhete_temporario, ultimo_bilhete,
                motivo_nao_consultado, motivo_nao_cancelado,
                motivo_nao_aberto, motivo_nao_reagendado,
                novo_status_bilhete, nova_data_portabilidade,
                responsavel_processamento, data_inicial_processamento,
                data_final_processamento, registro_valido,
                ajustes_registro, numero_acesso_valido, ajustes_numero_acesso
            FROM portabilidade_records
            WHERE status_ordem = 'Em Aprovisionamento' 
               OR status_bilhete = 'Em Aprovisionamento'
               OR status_ordem = 'Erro no Aprovisionamento'
               OR status_bilhete = 'Erro no Aprovisionamento'
            ORDER BY data_inicial_processamento DESC
            LIMIT 1000
        """)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    
    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("\n⚠ Nenhum registro em aprovisionamento encontrado!")
        return
    
    # [3] Carregar ObjectsLoader para verificar entrega
    print("[3] Carregando Relatório de Objetos para verificar entrega...")
    objects_loader = None
    arquivos_objetos = list(OBJECTS_PATH.glob("Relatorio_Objetos*.xlsx"))
    if arquivos_objetos:
        arquivo_objetos = max(arquivos_objetos, key=lambda x: x.stat().st_mtime)
        objects_loader = ObjectsLoader()
        count = objects_loader.load(str(arquivo_objetos))
        print(f"    >> {count} registros de logística carregados")
    else:
        print("    >> Relatório de Objetos não encontrado")
    
    # [3.1] Carregar Base Analítica para buscar Bluechip Status
    print("[3.1] Carregando Base Analítica...")
    base_analitica_loader = None
    if BASE_ANALITICA_PATH.exists():
        try:
            base_analitica_loader = BaseAnaliticaLoader(str(BASE_ANALITICA_PATH))
            count = base_analitica_loader.load()
            if count > 0:
                print(f"    >> {count} registros da base analítica carregados")
        except Exception as e:
            print(f"    >> Erro ao carregar base analítica: {e}")
            logger.warning(f"Erro ao carregar Base Analítica: {e}")
    else:
        print(f"    >> Arquivo base analítica não encontrado: {BASE_ANALITICA_PATH}")
    
    # [4] Converter para PortabilidadeRecord e filtrar entregues
    print("[4] Filtrando registros entregues...")
    from src.models.portabilidade import PortabilidadeRecord
    
    aprovisionados_entregues = []
    results_map = {}  # Simular results_map vazio para homologação
    
    for row in rows:
        record_dict = dict(zip(columns, row))
        
        # Criar record
        record = PortabilidadeRecord(
            cpf=record_dict.get('cpf', ''),
            numero_acesso=record_dict.get('numero_acesso', ''),
            numero_ordem=record_dict.get('numero_ordem', ''),
            codigo_externo=record_dict.get('codigo_externo', ''),
            status_bilhete=PortabilidadeStatus(record_dict['status_bilhete']) if record_dict.get('status_bilhete') else None,
            status_ordem=StatusOrdem(record_dict['status_ordem']) if record_dict.get('status_ordem') else None,
            operadora_doadora=record_dict.get('operadora_doadora'),
            data_portabilidade=datetime.fromisoformat(record_dict['data_portabilidade']) if record_dict.get('data_portabilidade') else None,
            motivo_recusa=record_dict.get('motivo_recusa'),
            motivo_cancelamento=record_dict.get('motivo_cancelamento'),
            preco_ordem=record_dict.get('preco_ordem'),
            numero_bilhete=record_dict.get('numero_bilhete'),
            numero_temporario=record_dict.get('numero_temporario'),
            bilhete_temporario=record_dict.get('bilhete_temporario'),
            ultimo_bilhete=bool(record_dict.get('ultimo_bilhete')) if record_dict.get('ultimo_bilhete') else None,
            motivo_nao_consultado=record_dict.get('motivo_nao_consultado'),
            motivo_nao_cancelado=record_dict.get('motivo_nao_cancelado'),
            motivo_nao_aberto=record_dict.get('motivo_nao_aberto'),
            motivo_nao_reagendado=record_dict.get('motivo_nao_reagendado'),
            novo_status_bilhete=record_dict.get('novo_status_bilhete'),
            nova_data_portabilidade=datetime.fromisoformat(record_dict['nova_data_portabilidade']) if record_dict.get('nova_data_portabilidade') else None,
            responsavel_processamento=record_dict.get('responsavel_processamento'),
            data_inicial_processamento=datetime.fromisoformat(record_dict['data_inicial_processamento']) if record_dict.get('data_inicial_processamento') else None,
            data_final_processamento=datetime.fromisoformat(record_dict['data_final_processamento']) if record_dict.get('data_final_processamento') else None,
            registro_valido=bool(record_dict.get('registro_valido')) if record_dict.get('registro_valido') else None,
            ajustes_registro=record_dict.get('ajustes_registro'),
            numero_acesso_valido=bool(record_dict.get('numero_acesso_valido')) if record_dict.get('numero_acesso_valido') else None,
            ajustes_numero_acesso=record_dict.get('ajustes_numero_acesso')
        )
        
        # Verificar Status da ordem: deve ser "Em Aprovisionamento" ou "Erro no Aprovisionamento"
        status_ordem_valido = False
        if record.status_ordem:
            status_ordem_str = str(record.status_ordem.value if hasattr(record.status_ordem, 'value') else record.status_ordem)
            if 'Em Aprovisionamento' in status_ordem_str or 'Erro no Aprovisionamento' in status_ordem_str:
                status_ordem_valido = True
        
        if not status_ordem_valido:
            continue
        
        # EXCLUIR registros com motivos específicos
        motivo_recusa = str(record.motivo_recusa or '').strip()
        motivo_cancelamento = str(record.motivo_cancelamento or '').strip()
        
        motivos_excluir = [
            'Rejeição do Cliente via SMS',
            'CPF Inválido',
            'Portabilidade de Número Vago',
            'Portabillidade de Número Vago',  # Com erro de digitação
            'Tipo de cliente inválido'
        ]
        
        # Verificar se algum motivo de exclusão está presente
        deve_excluir = False
        for motivo in motivos_excluir:
            if motivo.lower() in motivo_recusa.lower() or motivo.lower() in motivo_cancelamento.lower():
                deve_excluir = True
                break
        
        if deve_excluir:
            continue
        
        # Verificar se está entregue
        # PRIORIDADE: Última Ocorrência (Relatório de Objetos) > Base Analítica (Bluechip Status) > Status/Data Entrega
        is_entregue = False
        
        # PRIORIDADE 1: Verificar Última Ocorrência no ObjectsLoader (Relatório de Objetos)
        if objects_loader:
            obj_match = objects_loader.find_best_match(
                codigo_externo=record.codigo_externo,
                cpf=record.cpf
            )
            if obj_match:
                # Verificar Última Ocorrência (prioridade máxima)
                # Excluir "Entrega Cancelada" da contabilização
                if hasattr(obj_match, 'ultima_ocorrencia') and obj_match.ultima_ocorrencia:
                    ultima_ocorrencia_str = str(obj_match.ultima_ocorrencia).lower()
                    # Excluir entrega cancelada
                    if 'entrega cancelada' not in ultima_ocorrencia_str and 'cancelada' not in ultima_ocorrencia_str:
                        if any(termo in ultima_ocorrencia_str for termo in ['pedido entregue', 'entregue', '6']):
                            is_entregue = True
                
                # Se não encontrou em Última Ocorrência, verificar Status
                if not is_entregue and hasattr(obj_match, 'status') and obj_match.status:
                    status_str = str(obj_match.status).lower()
                    if any(termo in status_str for termo in ['pedido entregue', 'entregue', '6']):
                        is_entregue = True
                
                # Se não encontrou, verificar data de entrega
                if not is_entregue and hasattr(obj_match, 'data_entrega') and obj_match.data_entrega:
                    is_entregue = True
        
        # PRIORIDADE 2: Verificar na Base Analítica (Bluechip Status) se não encontrou ainda
        # Nota: A Base Analítica será verificada no CSVGenerator se necessário
        
        # Aplicar filtro: aprovisionamento E entregue
        if is_entregue:
            aprovisionados_entregues.append(record)
    
    print(f"    >> {len(aprovisionados_entregues)} registros em aprovisionamento E entregues")
    
    if not aprovisionados_entregues:
        print("\n⚠ Nenhum registro em aprovisionamento com entrega encontrado!")
        return
    
    # [5] Gerar arquivo de homologação
    print("[5] Gerando arquivo de homologação...")
    # Gerar em arquivo temporário primeiro para evitar problemas de permissão
    output_path = OUTPUT_TEMP
    
    if CSVGenerator.generate_aprovisionamentos_csv(
        aprovisionados_entregues,
        results_map,
        output_path,
        objects_loader,
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
        print(f"  Total de registros: {len(aprovisionados_entregues)}")
        print()
        print("=" * 70)
        print("HOMOLOGAÇÃO GERADA COM SUCESSO!")
        print("=" * 70)
    else:
        print("\n✗ ERRO ao gerar arquivo de homologação!")

if __name__ == "__main__":
    main()

