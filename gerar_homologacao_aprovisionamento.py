"""
Script para gerar arquivo de homologação de Aprovisionamento
Filtra registros em aprovisionamento (status Em Aprovisionamento) E entregue
Sincroniza com todas as tabelas do portabilidade.db
"""
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.utils.objects_loader import ObjectsLoader
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
from src.utils.csv_generator import CSVGenerator

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_aprovisionamento.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos (usar config centralizado)
try:
    from config import DB_PATH, OUTPUT_APROVISIONAMENTOS, PASTA_IMPORTACOES
    DB_PATH = DB_PATH
    OUTPUT_HOMOLOGACAO = Path("data/homologacao_aprovisionamento.xlsx")
    OBJECTS_PATH = Path(PASTA_IMPORTACOES)
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe
except ImportError:
    # Fallback se config.py não existir - usar caminho absoluto
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")
    OUTPUT_HOMOLOGACAO = Path(__file__).parent / "data" / "homologacao_aprovisionamento.xlsx"
    OBJECTS_PATH = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

OUTPUT_TEMP = Path("data/homologacao_aprovisionamento_temp.xlsx")

# Importar BaseAnaliticaLoader
from gerar_homologacao_wpp import BaseAnaliticaLoader

def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - APROVISIONAMENTO")
    print("=" * 70)
    print("Sincronizando com todas as tabelas do portabilidade.db")
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar registros em aprovisionamento sincronizando todas as tabelas
    print("[2] Buscando registros em aprovisionamento (sincronizando todas as tabelas)...")
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar quais tabelas existem
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas_existentes = [row[0] for row in cursor.fetchall()]
        tem_base_coverte = 'base_coverte_prop' in tabelas_existentes
        tem_relatorio_objetos = 'relatorio_objetos' in tabelas_existentes
        
        if tem_base_coverte:
            print("    >> Usando base_coverte_prop + portabilidade_records" + (" + relatorio_objetos" if tem_relatorio_objetos else ""))
        else:
            print("    >> Tabela base_coverte_prop não encontrada, usando apenas portabilidade_records")
        
        # Query sincronizada usando todas as tabelas disponíveis
        if tem_base_coverte:
            query = """
            SELECT DISTINCT
                -- CPF com fallback: base_coverte_prop > portabilidade_records > relatorio_objetos
                COALESCE(
                    NULLIF(TRIM(CAST(bc.cpf AS TEXT)), ''),
                    NULLIF(TRIM(CAST(pr.cpf AS TEXT)), ''),
                    NULLIF(TRIM(CAST(ro.documento AS TEXT)), ''),
                    ''
                ) AS cpf,
                
                -- Numero acesso: portabilidade_records (prioridade)
                COALESCE(pr.numero_acesso, '') AS numero_acesso,
                
                -- Numero ordem: portabilidade_records > base_coverte_prop
                COALESCE(pr.numero_ordem, bc.numero_ordem, '') AS numero_ordem,
                
                -- Codigo externo: base_coverte_prop > portabilidade_records > relatorio_objetos
                COALESCE(
                    NULLIF(TRIM(CAST(bc.proposta_isize AS TEXT)), ''),
                    NULLIF(TRIM(CAST(bc.codigo_externo AS TEXT)), ''),
                    NULLIF(TRIM(CAST(pr.codigo_externo AS TEXT)), ''),
                    NULLIF(TRIM(CAST(ro.codigo_externo AS TEXT)), ''),
                    ''
                ) AS codigo_externo,
                
                -- Status de portabilidade (portabilidade_records)
                COALESCE(pr.status_bilhete, '') AS status_bilhete,
                COALESCE(pr.status_ordem, '') AS status_ordem,
                COALESCE(pr.operadora_doadora, '') AS operadora_doadora,
                
                -- Datas
                COALESCE(pr.data_portabilidade, '') AS data_portabilidade,
                bc.data_venda AS data_venda,
                
                -- Motivos
                COALESCE(pr.motivo_recusa, '') AS motivo_recusa,
                COALESCE(pr.motivo_cancelamento, '') AS motivo_cancelamento,
                
                -- Preço
                COALESCE(pr.preco_ordem, '') AS preco_ordem,
                
                -- Campos adicionais de portabilidade_records
                pr.numero_bilhete,
                pr.numero_temporario,
                pr.bilhete_temporario,
                pr.ultimo_bilhete,
                pr.motivo_nao_consultado,
                pr.motivo_nao_cancelado,
                pr.motivo_nao_aberto,
                pr.motivo_nao_reagendado,
                pr.novo_status_bilhete,
                pr.nova_data_portabilidade,
                pr.responsavel_processamento,
                pr.data_inicial_processamento,
                pr.data_final_processamento,
                pr.registro_valido,
                pr.ajustes_registro,
                pr.numero_acesso_valido,
                pr.ajustes_numero_acesso,
                
                -- Dados adicionais de base_coverte_prop
                bc.cliente_nome,
                bc.telefone_portado,
                bc.plano,
                bc.crivo_vendas,
                bc.bluechip_status,
                
                -- Dados de logística (relatorio_objetos)
                ro.nu_pedido AS ro_nu_pedido,
                ro.rastreio AS ro_rastreio,
                ro.status AS ro_status_entrega,
                ro.transportadora AS ro_transportadora,
                ro.ultima_ocorrencia AS ro_ultima_ocorrencia,
                ro.data_entrega AS ro_data_entrega,
                ro.iccid AS ro_iccid
                
            FROM portabilidade_records pr
            LEFT JOIN base_coverte_prop bc ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
            )
            LEFT JOIN relatorio_objetos ro ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), CAST(pr.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
            )
            WHERE 
                -- Filtro: APENAS "Em Aprovisionamento" (NÃO incluir "Erro no Aprovisionamento")
                (
                    pr.status_ordem = 'Em Aprovisionamento' 
                    OR pr.status_bilhete = 'Em Aprovisionamento'
                )
            ORDER BY 
                CASE 
                    WHEN bc.data_venda IS NOT NULL 
                         AND TRIM(CAST(bc.data_venda AS TEXT)) != ''
                         AND (SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 5, 1) = '-' OR LENGTH(TRIM(CAST(bc.data_venda AS TEXT))) >= 10)
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                    THEN date(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10))
                    WHEN bc.data_venda IS NOT NULL 
                         AND TRIM(CAST(bc.data_venda AS TEXT)) != ''
                         AND LENGTH(TRIM(CAST(bc.data_venda AS TEXT))) >= 10
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 3, 1) = '/'
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 6, 1) = '/'
                    THEN date(
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 7, 4) || '-' || 
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 4, 2) || '-' || 
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 2)
                    )
                    ELSE date('1900-01-01')
                END DESC,
                pr.data_inicial_processamento DESC
            LIMIT 5000
            """
        else:
            # Fallback: usar apenas portabilidade_records
            query = """
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
                ajustes_registro, numero_acesso_valido, ajustes_numero_acesso,
                NULL AS data_venda,
                NULL AS cliente_nome,
                NULL AS telefone_portado,
                NULL AS plano,
                NULL AS crivo_vendas,
                NULL AS bluechip_status,
                NULL AS ro_nu_pedido,
                NULL AS ro_rastreio,
                NULL AS ro_status_entrega,
                NULL AS ro_transportadora,
                NULL AS ro_ultima_ocorrencia,
                NULL AS ro_data_entrega,
                NULL AS ro_iccid
            FROM portabilidade_records
            WHERE status_ordem = 'Em Aprovisionamento' 
               OR status_bilhete = 'Em Aprovisionamento'
            ORDER BY data_inicial_processamento DESC
            LIMIT 1000
            """
        
        cursor.execute(query)
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
    
    # [3.1] Verificar Base Analítica (banco unificado)
    print("[3.1] Verificando Base Analítica (banco unificado)...")
    base_analitica_loader = None
    tem_base_coverte = False
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM base_coverte_prop")
            total_coverte = cursor.fetchone()[0]
            tem_base_coverte = total_coverte > 0
            if tem_base_coverte:
                print(f"    >> {total_coverte:,} registros encontrados na base_coverte_prop")
    except Exception as e:
        print(f"    >> Erro ao verificar tabelas: {e}")
    
    # [4] Converter para PortabilidadeRecord e filtrar entregues
    print("[4] Filtrando registros entregues...")
    from src.models.portabilidade import PortabilidadeRecord
    
    aprovisionados_entregues = []
    results_map = {}  # Simular results_map vazio para homologação
    
    for row in rows:
        record_dict = dict(zip(columns, row))
        
        # Criar record
        try:
            record = PortabilidadeRecord(
                cpf=str(record_dict.get('cpf', '') or '').strip(),
                numero_acesso=str(record_dict.get('numero_acesso', '') or '').strip(),
                numero_ordem=str(record_dict.get('numero_ordem', '') or '').strip(),
                codigo_externo=str(record_dict.get('codigo_externo', '') or '').strip(),
                status_bilhete=PortabilidadeStatus(record_dict['status_bilhete']) if record_dict.get('status_bilhete') and str(record_dict['status_bilhete']).strip() else None,
                status_ordem=StatusOrdem(record_dict['status_ordem']) if record_dict.get('status_ordem') and str(record_dict['status_ordem']).strip() else None,
                operadora_doadora=str(record_dict.get('operadora_doadora', '') or '').strip() if record_dict.get('operadora_doadora') else None,
                data_portabilidade=datetime.fromisoformat(record_dict['data_portabilidade']) if record_dict.get('data_portabilidade') else None,
                motivo_recusa=str(record_dict.get('motivo_recusa', '') or '').strip() if record_dict.get('motivo_recusa') else None,
                motivo_cancelamento=str(record_dict.get('motivo_cancelamento', '') or '').strip() if record_dict.get('motivo_cancelamento') else None,
                preco_ordem=str(record_dict.get('preco_ordem', '') or '').strip() if record_dict.get('preco_ordem') else None,
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
        except Exception as e:
            logger.error(f"Erro ao criar record: {e} - Dados: {record_dict}")
            continue
        
        # Verificar Status da ordem: deve ser "Em Aprovisionamento" (não "Erro no Aprovisionamento")
        status_ordem_valido = False
        if record.status_ordem:
            status_ordem_str = str(record.status_ordem.value if hasattr(record.status_ordem, 'value') else record.status_ordem)
            if 'Em Aprovisionamento' in status_ordem_str and 'Erro' not in status_ordem_str:
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
        
        # Verificar se está entregue usando dados sincronizados
        is_entregue = False
        
        # PRIORIDADE 1: Verificar dados de relatorio_objetos (já sincronizados na query)
        if record_dict.get('ro_ultima_ocorrencia'):
            ultima_ocorrencia_str = str(record_dict['ro_ultima_ocorrencia']).lower()
            if 'entrega cancelada' not in ultima_ocorrencia_str and 'cancelada' not in ultima_ocorrencia_str:
                if any(termo in ultima_ocorrencia_str for termo in ['pedido entregue', 'entregue', '6']):
                    is_entregue = True
        
        if not is_entregue and record_dict.get('ro_status_entrega'):
            status_str = str(record_dict['ro_status_entrega']).lower()
            if any(termo in status_str for termo in ['pedido entregue', 'entregue', '6']):
                is_entregue = True
        
        if not is_entregue and record_dict.get('ro_data_entrega'):
            is_entregue = True
        
        if not is_entregue and record_dict.get('ro_iccid'):
            iccid_str = str(record_dict['ro_iccid']).strip()
            if iccid_str and iccid_str.lower() != 'nan':
                is_entregue = True
        
        # PRIORIDADE 2: Verificar no ObjectsLoader se ainda não encontrou
        if not is_entregue and objects_loader:
            obj_match = objects_loader.find_best_match(
                codigo_externo=record.codigo_externo,
                cpf=record.cpf
            )
            if obj_match:
                if hasattr(obj_match, 'ultima_ocorrencia') and obj_match.ultima_ocorrencia:
                    ultima_ocorrencia_str = str(obj_match.ultima_ocorrencia).lower()
                    if 'entrega cancelada' not in ultima_ocorrencia_str and 'cancelada' not in ultima_ocorrencia_str:
                        if any(termo in ultima_ocorrencia_str for termo in ['pedido entregue', 'entregue', '6']):
                            is_entregue = True
                
                if not is_entregue and hasattr(obj_match, 'status') and obj_match.status:
                    status_str = str(obj_match.status).lower()
                    if any(termo in status_str for termo in ['pedido entregue', 'entregue', '6']):
                        is_entregue = True
                
                if not is_entregue and hasattr(obj_match, 'data_entrega') and obj_match.data_entrega:
                    is_entregue = True
                
                if not is_entregue:
                    if hasattr(obj_match, 'iccid') and obj_match.iccid:
                        iccid_str = str(obj_match.iccid).strip()
                        if iccid_str and iccid_str.lower() != 'nan':
                            is_entregue = True
        
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
    
    # Gerar CSV primeiro
    output_csv = Path("data/homologacao_aprovisionamento_temp.csv")
    # O método generate_aprovisionamentos_csv faz uma nova verificação de entrega
    # Mas já filtramos os registros entregues, então vamos gerar diretamente
    try:
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        
        import csv
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            
            # Cabeçalho
            headers = [
                'Cpf', 'Número de acesso', 'Número da ordem', 'Código externo',
                'ICCID', 'ToutBox', 'Número do bilhete', 'Status do bilhete',
                'Operadora doadora', 'Data da portabilidade', 'Motivo da recusa',
                'Motivo do cancelamento', 'Último bilhete de portabilidade?',
                'Status da ordem', 'Preço da ordem', 'Data da conclusão da ordem',
                'Motivo de não ter sido consultado', 'Motivo de não ter sido cancelado',
                'Motivo de não ter sido aberto', 'Motivo de não ter sido reagendado',
                'Novo status do bilhete', 'Nova data da portabilidade',
                'Responsável pelo processamento', 'Data inicial do processamento',
                'Data final do processamento', 'Registro válido?', 'Ajustes registro',
                'Número de acesso válido?', 'Ajustes número de acesso',
                'Status da entrega', 'Data da entrega', 'Parâmetro de Identificação',
                'Data Última Atualização Coleta', 'Tipo de Venda'
            ]
            writer.writerow(headers)
            
            # Dados
            for record in aprovisionados_entregues:
                def safe_str(value, default=''):
                    return str(value) if value is not None else default
                
                def safe_date(value, default=''):
                    if value is None:
                        return default
                    try:
                        if isinstance(value, datetime):
                            return value.strftime("%d/%m/%Y")
                        return str(value)
                    except (ValueError, TypeError, AttributeError):
                        return default
                
                def safe_enum(value, default=''):
                    if value is None:
                        return default
                    try:
                        return value.value if hasattr(value, 'value') else str(value)
                    except (ValueError, TypeError, AttributeError):
                        return default
                
                def safe_bool(value, default=''):
                    if value is None:
                        return default
                    return 'Sim' if value else 'Não'
                
                # Tipo de venda
                tipo_venda = 'Nova Linha'
                if record.operadora_doadora and str(record.operadora_doadora).strip():
                    tipo_venda = 'Portabilidade'
                elif record.data_portabilidade:
                    tipo_venda = 'Portabilidade'
                
                # Dados de entrega (já verificados)
                status_entrega = 'Entregue'
                data_entrega = safe_date(record.data_entrega) if hasattr(record, 'data_entrega') else ''
                iccid = safe_str(record.iccid) if hasattr(record, 'iccid') else ''
                
                row = [
                    safe_str(record.cpf),
                    safe_str(record.numero_acesso),
                    safe_str(record.numero_ordem),
                    safe_str(record.codigo_externo),
                    iccid,
                    '',  # ToutBox
                    safe_str(record.numero_bilhete),
                    safe_enum(record.status_bilhete),
                    safe_str(record.operadora_doadora),
                    safe_date(record.data_portabilidade),
                    safe_str(record.motivo_recusa),
                    safe_str(record.motivo_cancelamento),
                    safe_bool(record.ultimo_bilhete),
                    safe_enum(record.status_ordem),
                    safe_str(record.preco_ordem),
                    safe_date(record.data_final_processamento) if hasattr(record, 'data_final_processamento') else '',
                    safe_str(record.motivo_nao_consultado) if hasattr(record, 'motivo_nao_consultado') else '',
                    safe_str(record.motivo_nao_cancelado) if hasattr(record, 'motivo_nao_cancelado') else '',
                    safe_str(record.motivo_nao_aberto) if hasattr(record, 'motivo_nao_aberto') else '',
                    safe_str(record.motivo_nao_reagendado) if hasattr(record, 'motivo_nao_reagendado') else '',
                    safe_str(record.novo_status_bilhete) if hasattr(record, 'novo_status_bilhete') else '',
                    safe_date(record.nova_data_portabilidade) if hasattr(record, 'nova_data_portabilidade') else '',
                    safe_str(record.responsavel_processamento) if hasattr(record, 'responsavel_processamento') else '',
                    safe_date(record.data_inicial_processamento) if hasattr(record, 'data_inicial_processamento') else '',
                    safe_date(record.data_final_processamento) if hasattr(record, 'data_final_processamento') else '',
                    safe_bool(record.registro_valido) if hasattr(record, 'registro_valido') else '',
                    safe_str(record.ajustes_registro) if hasattr(record, 'ajustes_registro') else '',
                    safe_bool(record.numero_acesso_valido) if hasattr(record, 'numero_acesso_valido') else '',
                    safe_str(record.ajustes_numero_acesso) if hasattr(record, 'ajustes_numero_acesso') else '',
                    status_entrega,
                    data_entrega,
                    '',  # Parâmetro de Identificação
                    '',  # Data Última Atualização Coleta
                    tipo_venda
                ]
                writer.writerow(row)
        
        # Converter CSV para XLSX
        try:
            # Ler CSV
            df = pd.read_csv(output_csv, delimiter=';', encoding='utf-8-sig')
            
            # Salvar como XLSX
            if OUTPUT_HOMOLOGACAO.exists():
                OUTPUT_HOMOLOGACAO.unlink()
            df.to_excel(OUTPUT_HOMOLOGACAO, index=False, engine='openpyxl')
            
            # Remover CSV temporário
            if output_csv.exists():
                output_csv.unlink()
            
            output_path = OUTPUT_HOMOLOGACAO
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
        except Exception as e:
            logger.error(f"Erro ao converter CSV para XLSX: {e}")
            print(f"\n✗ ERRO ao converter arquivo para XLSX: {e}")
            if output_csv.exists():
                print(f"    >> Arquivo CSV gerado em: {output_csv}")
    except Exception as e:
        logger.error(f"Erro ao gerar arquivo CSV: {e}")
        print(f"\n✗ ERRO ao gerar arquivo de homologação: {e}")

if __name__ == "__main__":
    main()
