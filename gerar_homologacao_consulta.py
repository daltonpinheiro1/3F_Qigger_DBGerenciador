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
import pandas as pd

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

# Importar BaseAnaliticaLoader
try:
    from gerar_homologacao_wpp import BaseAnaliticaLoader
except ImportError:
    BaseAnaliticaLoader = None
    logger.warning("BaseAnaliticaLoader não encontrado. Filtro de crivo aprovada não estará disponível.")

# Caminhos - usar config.py centralizado
try:
    from config import DB_PATH
except ImportError:
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")

OUTPUT_PATH = Path("data/homologacao_consulta.xlsx")
BASE_ANALITICA_PATH = Path("/dev/null")  # Agora usa base_coverte_prop do banco


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


def is_crivo_aprovada(codigo_externo: str, cpf: str, base_analitica_loader) -> bool:
    """Verifica se o registro tem crivo aprovada (Status venda = APROVADA)"""
    if not base_analitica_loader or not base_analitica_loader.is_loaded:
        return True  # Se não tiver base analítica, não filtra (mantém o registro)
    
    # Buscar na base analítica
    base_match = base_analitica_loader.find_best_match(
        codigo_externo=codigo_externo,
        cpf=cpf
    )
    
    if base_match is None:
        return False  # Se não encontrar na base analítica, exclui
    
    # Verificar Status venda
    status_venda = base_match.get('Status venda') or base_match.get('Status_venda') or base_match.get('Status Venda')
    
    if status_venda is None:
        return False  # Se não tiver status venda, exclui
    
    status_venda_str = str(status_venda).strip().upper()
    
    # Considerar aprovada se for "APROVADA" (case insensitive)
    return status_venda_str == "APROVADA"


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
    print("  - Crivo aprovada (Status venda = APROVADA)")
    print()
    
    # Conectar ao banco
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # Carregar base analítica para verificar crivo aprovada
    print("[1.1] Carregando base analítica...")
    base_analitica_loader = None
    if BaseAnaliticaLoader and BASE_ANALITICA_PATH.exists():
        try:
            base_analitica_loader = BaseAnaliticaLoader(str(BASE_ANALITICA_PATH))
            count = base_analitica_loader.load()
            print(f"    >> Base analítica carregada: {count:,} registros")
        except Exception as e:
            print(f"    >> ⚠ Erro ao carregar base analítica: {e}")
            logger.warning(f"Erro ao carregar base analítica: {e}")
            base_analitica_loader = None
    else:
        if not BASE_ANALITICA_PATH.exists():
            print(f"    >> ⚠ Base analítica não encontrada: {BASE_ANALITICA_PATH}")
            print("    >> Filtro de crivo aprovada será ignorado")
        else:
            print("    >> ⚠ BaseAnaliticaLoader não disponível")
            print("    >> Filtro de crivo aprovada será ignorado")
    
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
    print(f"    >> Data de hoje: {hoje.strftime('%d/%m/%Y')}")
    print(f"    >> Considerando datas de: 01/{mes_atual:02d}/{ano_atual} até 31/{mes_atual:02d}/{ano_atual}")
    
    registros_filtrados = []
    stats = {
        'total': len(all_records),
        'sem_data_conectada': 0,
        'fora_mes_vigente': 0,
        'com_rejeicao_sms': 0,
        'status_excluido': 0,
        'sem_crivo_aprovada': 0,
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
            # Log de exemplo para debug (apenas os primeiros 3)
            if stats['fora_mes_vigente'] <= 3:
                logger.debug(f"Registro fora do mês vigente: CPF={record.get('cpf')}, Data={data_conectada.strftime('%d/%m/%Y')}")
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
        
        # Verificar crivo aprovada (Status venda = APROVADA)
        codigo_externo = record.get('codigo_externo', '').strip()
        cpf = record.get('cpf', '').strip()
        
        if not is_crivo_aprovada(codigo_externo, cpf, base_analitica_loader):
            stats['sem_crivo_aprovada'] += 1
            continue
        
        # Registro aprovado
        registros_filtrados.append(record)
        stats['aprovados'] += 1
    
    print(f"    >> Registros sem data conectada: {stats['sem_data_conectada']:,}")
    print(f"    >> Registros fora do mês vigente: {stats['fora_mes_vigente']:,}")
    print(f"    >> Registros com rejeição de SMS: {stats['com_rejeicao_sms']:,}")
    print(f"    >> Registros com status excluído: {stats['status_excluido']:,}")
    print(f"    >> Registros sem crivo aprovada: {stats['sem_crivo_aprovada']:,}")
    print(f"    >> Registros aprovados: {stats['aprovados']:,}")
    
    if not registros_filtrados:
        print()
        print("⚠ Nenhum registro encontrado com os critérios especificados!")
        return 1
    
    # Gerar arquivo Excel
    print()
    print("[4] Gerando arquivo Excel...")
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    # Ordenar por data conectada (mais recente primeiro)
    registros_filtrados.sort(
        key=lambda r: parse_date(r.get('data_inicial_processamento', '')) or date.min,
        reverse=True
    )
    
    # Preparar dados para DataFrame
    dados = []
    for record in registros_filtrados:
        cpf = record.get('cpf', '').strip()
        numero_acesso = record.get('numero_acesso', '').strip()
        numero_ordem = record.get('numero_ordem', '').strip()
        codigo_externo = record.get('codigo_externo', '').strip()
        
        # Se número da ordem estiver vazio ou for igual ao código externo (id_isize),
        # tentar buscar da base analítica
        if (not numero_ordem or numero_ordem == codigo_externo) and base_analitica_loader and base_analitica_loader.is_loaded:
            base_match = base_analitica_loader.find_best_match(
                codigo_externo=codigo_externo,
                cpf=cpf
            )
            
            if base_match is not None:
                # Tentar buscar número OS ou número da ordem da base analítica
                numero_os = base_match.get('Numero OS') or base_match.get('Numero_OS') or base_match.get('Número OS') or base_match.get('Número_OS')
                numero_ordem_base = base_match.get('Numero Ordem') or base_match.get('Numero_Ordem') or base_match.get('Número Ordem') or base_match.get('Número_Ordem')
                id_erp = base_match.get('ID ERP') or base_match.get('ID_ERP') or base_match.get('Id ERP') or base_match.get('Id_ERP')
                
                # Prioridade: Numero OS > Numero Ordem > ID ERP
                if numero_os and pd.notna(numero_os):
                    numero_ordem = str(numero_os).strip()
                elif numero_ordem_base and pd.notna(numero_ordem_base):
                    numero_ordem = str(numero_ordem_base).strip()
                elif id_erp and pd.notna(id_erp):
                    numero_ordem = str(id_erp).strip()
        
        # Se ainda estiver vazio ou igual ao código externo, manter vazio (não usar código externo como fallback)
        if numero_ordem == codigo_externo:
            numero_ordem = ''
        
        dados.append({
            'Cpf': cpf,
            'Número de acesso': numero_acesso,
            'Número da ordem': numero_ordem,
            'Código externo': codigo_externo
        })
    
    # Criar DataFrame
    df = pd.DataFrame(dados)
    
    # Garantir que as colunas estejam na ordem correta
    colunas = ['Cpf', 'Número de acesso', 'Número da ordem', 'Código externo']
    df = df[colunas]
    
    # Salvar em Excel
    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Consulta')
        
        # Ajustar largura das colunas e formatar como valores
        worksheet = writer.sheets['Consulta']
        worksheet.column_dimensions['A'].width = 15  # CPF
        worksheet.column_dimensions['B'].width = 20   # Número de acesso
        worksheet.column_dimensions['C'].width = 25   # Número da ordem
        worksheet.column_dimensions['D'].width = 15   # Código externo
        
        # Formatar células como texto para preservar zeros à esquerda e evitar notação científica
        from openpyxl.styles import NamedStyle
        text_style = NamedStyle(name="text_style", number_format='@')
        
        # Aplicar formato de texto para todas as colunas (preserva valores como estão)
        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for cell in row:
                cell.number_format = '@'  # Formato texto
    
    print(f"    >> Arquivo gerado: {OUTPUT_PATH}")
    print(f"    >> Total de registros: {len(registros_filtrados):,}")
    print(f"    >> Formato: Excel (.xlsx)")
    
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
    print(f"  - Sem crivo aprovada: {stats['sem_crivo_aprovada']:,}")
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

