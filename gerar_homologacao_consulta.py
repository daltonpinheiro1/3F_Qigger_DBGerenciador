"""
Script para gerar arquivo de homologação de consulta
Filtra vendas novas do mês vigente que não possuem rejeição de SMS
e não estão portadas, falha parcial ou antigas
"""
import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Any, Optional

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import csv

# Configurar logging
Path('logs').mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_consulta.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager

# Caminhos
DB_PATH = "data/portabilidade.db"
OUTPUT_PATH = Path("data/homologacao_consulta.csv")


def parse_date(date_str: str) -> Optional[date]:
    """Parse de data em vários formatos"""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%d/%m/%Y',
        '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.date()
        except (ValueError, TypeError):
            continue
    
    return None


def is_current_month(date_value: Any) -> bool:
    """Verifica se a data é do mês vigente"""
    if not date_value:
        return False
    
    parsed_date = None
    if isinstance(date_value, date):
        parsed_date = date_value
    elif isinstance(date_value, datetime):
        parsed_date = date_value.date()
    elif isinstance(date_value, str):
        parsed_date = parse_date(date_value)
    
    if not parsed_date:
        return False
    
    today = date.today()
    return parsed_date.year == today.year and parsed_date.month == today.month


def has_sms_rejection(motivo_recusa: str, motivo_cancelamento: str) -> bool:
    """Verifica se possui rejeição de SMS"""
    sms_keywords = [
        "Rejeição do Cliente via SMS",
        "rejeição do cliente via sms",
        "Rejeição do cliente via SMS",
        "REJEIÇÃO DO CLIENTE VIA SMS"
    ]
    
    motivo_recusa_str = str(motivo_recusa or "").strip()
    motivo_cancelamento_str = str(motivo_cancelamento or "").strip()
    
    for keyword in sms_keywords:
        if keyword in motivo_recusa_str or keyword in motivo_cancelamento_str:
            return True
    
    return False


def is_excluded_status(status_bilhete: str) -> bool:
    """Verifica se o status deve ser excluído (portado, falha parcial, antigo)"""
    if not status_bilhete:
        return False
    
    status_str = str(status_bilhete).strip()
    
    excluded_statuses = [
        "Portado",
        "Falha Parcial",
        "falha parcial",
        "FALHA PARCIAL"
    ]
    
    return status_str in excluded_statuses


def gerar_homologacao_consulta():
    """Gera arquivo de homologação de consulta"""
    
    print("=" * 70)
    print("GERAÇÃO DE HOMOLOGAÇÃO DE CONSULTA")
    print("=" * 70)
    print()
    print("Critérios de filtro:")
    print("  - Vendas novas (data conectada do mês vigente)")
    print("  - Sem rejeição de SMS")
    print("  - Não portado, falha parcial ou antigo")
    print()
    
    # Conectar ao banco
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # Buscar todos os registros
    print("[2] Buscando registros do banco...")
    all_records = db_manager.get_all_records()
    print(f"    >> Total de registros no banco: {len(all_records):,}")
    
    # Filtrar registros
    print("[3] Aplicando filtros...")
    
    hoje = date.today()
    mes_atual = hoje.month
    ano_atual = hoje.year
    
    print(f"    >> Mês vigente: {mes_atual:02d}/{ano_atual}")
    
    registros_filtrados = []
    stats = {
        'total': len(all_records),
        'sem_data_conectada': 0,
        'fora_mes_vigente': 0,
        'com_rejeicao_sms': 0,
        'status_excluido': 0,
        'aprovados': 0
    }
    
    for record in all_records:
        # Verificar data conectada (data_inicial_processamento)
        data_conectada_str = record.get('data_inicial_processamento')
        data_conectada = None
        
        if data_conectada_str:
            data_conectada = parse_date(data_conectada_str)
        
        # Se não tiver data conectada, pular
        if not data_conectada:
            stats['sem_data_conectada'] += 1
            continue
        
        # Verificar se é do mês vigente
        if not is_current_month(data_conectada):
            stats['fora_mes_vigente'] += 1
            continue
        
        # Verificar rejeição de SMS
        motivo_recusa = record.get('motivo_recusa', '')
        motivo_cancelamento = record.get('motivo_cancelamento', '')
        
        if has_sms_rejection(motivo_recusa, motivo_cancelamento):
            stats['com_rejeicao_sms'] += 1
            continue
        
        # Verificar status excluído
        status_bilhete = record.get('status_bilhete', '')
        if is_excluded_status(status_bilhete):
            stats['status_excluido'] += 1
            continue
        
        # Registro aprovado
        registros_filtrados.append(record)
        stats['aprovados'] += 1
    
    print(f"    >> Registros sem data conectada: {stats['sem_data_conectada']:,}")
    print(f"    >> Registros fora do mês vigente: {stats['fora_mes_vigente']:,}")
    print(f"    >> Registros com rejeição de SMS: {stats['com_rejeicao_sms']:,}")
    print(f"    >> Registros com status excluído: {stats['status_excluido']:,}")
    print(f"    >> Registros aprovados: {stats['aprovados']:,}")
    
    if not registros_filtrados:
        print()
        print("⚠ Nenhum registro encontrado com os critérios especificados!")
        return 1
    
    # Gerar CSV
    print()
    print("[4] Gerando arquivo CSV...")
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Ordenar por data conectada (mais recente primeiro)
    registros_filtrados.sort(
        key=lambda r: parse_date(r.get('data_inicial_processamento', '')) or date.min,
        reverse=True
    )
    
    with open(OUTPUT_PATH, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter='\t')  # Usar tab como separador (padrão Excel)
        
        # Cabeçalho
        writer.writerow([
            'Cpf',
            'Número de acesso',
            'Número da ordem',
            'Código externo'
        ])
        
        # Dados
        for record in registros_filtrados:
            writer.writerow([
                record.get('cpf', '').strip(),
                record.get('numero_acesso', '').strip(),
                record.get('numero_ordem', '').strip(),
                record.get('codigo_externo', '').strip()
            ])
    
    print(f"    >> Arquivo gerado: {OUTPUT_PATH}")
    print(f"    >> Total de registros: {len(registros_filtrados):,}")
    
    # Estatísticas finais
    print()
    print("=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"Total de registros no banco: {stats['total']:,}")
    print(f"Registros filtrados: {len(registros_filtrados):,}")
    print()
    print("Filtros aplicados:")
    print(f"  - Sem data conectada: {stats['sem_data_conectada']:,}")
    print(f"  - Fora do mês vigente: {stats['fora_mes_vigente']:,}")
    print(f"  - Com rejeição de SMS: {stats['com_rejeicao_sms']:,}")
    print(f"  - Status excluído: {stats['status_excluido']:,}")
    print()
    print(f"✓ Arquivo gerado com sucesso: {OUTPUT_PATH.absolute()}")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    try:
        sys.exit(gerar_homologacao_consulta())
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"ERRO FATAL: {e}")
        logger.error(f"Erro fatal: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

