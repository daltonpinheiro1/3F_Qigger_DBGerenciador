"""
Script para gerar arquivo de homologação de Erro no Aprovisionamento
Regra: proposta (única) + algum histórico de entrega + status_ordem = Erro no Aprovisionamento → retorna no arquivo.
Sincroniza com todas as tabelas do portabilidade.db
"""
from pathlib import Path
from datetime import datetime, timedelta

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.utils.objects_loader import ObjectsLoader
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
from src.utils.validar_processamento import filtrar_registros_validos, obter_estatisticas_validacao
from src.utils.progress_bar import ProgressBar
from src.utils.data_integrity import sanitizar_valor, validar_integridade_linha

# Importar QueriesV2 para código path V2 (primário) com fallback legado
try:
    from config import DB_V2_PATH
    from src.database.queries_v2 import QueriesV2
    _V2_AVAILABLE = bool(DB_V2_PATH) and Path(DB_V2_PATH).exists()
except (ImportError, Exception):
    DB_V2_PATH = None
    _V2_AVAILABLE = False

# Respeitar flags --forcar-legado / --forcar-v2 propagadas via env vars
import os as _os
if _os.environ.get('QIGGER_FORCAR_LEGADO') == '1':
    _V2_AVAILABLE = False
elif _os.environ.get('QIGGER_FORCAR_V2') == '1' and DB_V2_PATH:
    _V2_AVAILABLE = True

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/homologacao_erro_aprovisionamento.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos (usar config centralizado)
try:
    from config import DB_PATH, PASTA_IMPORTACOES
    DB_PATH = DB_PATH
    OUTPUT_HOMOLOGACAO = Path("data/homologacao_erro_aprovisionamento.xlsx")
    OBJECTS_PATH = Path(PASTA_IMPORTACOES)
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe
except ImportError:
    # Fallback se config.py não existir
    DB_PATH = "data/portabilidade.db"
    OUTPUT_HOMOLOGACAO = Path("data/homologacao_erro_aprovisionamento.xlsx")
    OBJECTS_PATH = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

OUTPUT_TEMP = Path("data/homologacao_erro_aprovisionamento_temp.xlsx")

# Importar BaseAnaliticaLoader
from gerar_homologacao_wpp import BaseAnaliticaLoader

def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - ERRO NO APROVISIONAMENTO")
    print("=" * 70)
    print("Sincronizando com todas as tabelas do portabilidade.db")
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar registros com erro no aprovisionamento sincronizando todas as tabelas
    print("[2] Buscando registros com erro no aprovisionamento (sincronizando todas as tabelas)...")
    DIAS_LIMITE = 90
    data_limite = (datetime.now() - timedelta(days=DIAS_LIMITE)).strftime('%Y-%m-%d')
    filtro_data_sql = ">= '" + data_limite + "'"
    print(f"    >> Filtro: últimos {DIAS_LIMITE} dias (a partir de {data_limite}) | Ordenação: mais recente primeiro")

    # --- V2 como fonte PRIMÁRIA, legado como fallback ---
    rows = None
    columns = None
    _usou_v2 = False

    if _V2_AVAILABLE:
        try:
            import sqlite3 as _sqlite3
            # Verificar existência da view vw_consulta_siebel_corrente antes de consultar
            _conn_check = _sqlite3.connect(DB_V2_PATH)
            try:
                _view_ok = _conn_check.execute(
                    "SELECT name FROM sqlite_master WHERE type='view' AND name='vw_consulta_siebel_corrente'"
                ).fetchone() is not None
            finally:
                _conn_check.close()

            if not _view_ok:
                logger.error(
                    "[V2] View vw_consulta_siebel_corrente não encontrada em %s — fallback para legado",
                    DB_V2_PATH,
                )
                print("    >> [V2] ❌ View vw_consulta_siebel_corrente ausente no banco v2, usando fallback legado")
            else:
                print("    >> [V2] Banco v2 detectado, usando QueriesV2 como fonte primária...")
                queries_v2 = QueriesV2(DB_V2_PATH)
                registros_v2 = queries_v2.buscar_registros_erro_aprovisionamento(dias_limite=DIAS_LIMITE)
                if registros_v2:
                    columns = list(registros_v2[0].keys())
                    rows = [tuple(r[c] for c in columns) for r in registros_v2]
                    _usou_v2 = True
                    logger.info("[V2] ✅ %d registros obtidos via QueriesV2.buscar_registros_erro_aprovisionamento()", len(rows))
                    print(f"    >> [V2] ✅ {len(rows)} registros obtidos via QueriesV2")
                else:
                    logger.warning("[V2] ⚠ QueriesV2.buscar_registros_erro_aprovisionamento() retornou 0 registros — ATENÇÃO: V2 pode estar com dados desatualizados ou cache vazia. Usando fallback legado.")
                    print("    >> [V2] ⚠ 0 registros retornados (cache desatualizada?), usando fallback legado")
        except Exception as e:
            logger.error("[V2] Erro ao usar QueriesV2, usando fallback legado: %s", e, exc_info=True)
            print(f"    >> [V2] ❌ Fallback para legado: {e}")
    else:
        logger.info("[V2] Banco v2 não disponível (%s), usando legado", DB_V2_PATH)
        print("    >> [V2] Banco v2 não disponível, usando legado")

    # --- Fallback legado: só executa se V2 não retornou resultados ---
    if not _usou_v2:
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
            # Incluir TODAS as linhas com status Erro no Aprovisionamento (vários numero_acesso/numero_ordem por codigo_externo)
            if tem_base_coverte:
                query = """
                WITH
                -- Para Erro no Aprovisionamento: incluir TODAS as linhas com esse status (vários numero_acesso/numero_ordem por codigo_externo)
                -- CTE para contar classificações relevantes (com o status filtrado)
                contagem_classificacoes AS (
                    SELECT 
                        codigo_externo,
                        COUNT(*) as total_classificacoes
                    FROM portabilidade_records
                    WHERE (status_ordem = 'Erro no Aprovisionamento' OR status_bilhete = 'Erro no Aprovisionamento')
                    GROUP BY codigo_externo
                ),
                -- Subquery para pegar o novo_status_bilhete mais recente não-nulo
                ultimo_status_bilhete AS (
                    SELECT 
                        pr.codigo_externo,
                        pr.novo_status_bilhete as ultimo_novo_status_bilhete
                    FROM portabilidade_records pr
                    INNER JOIN (
                        SELECT codigo_externo, MAX(id) as max_id
                        FROM portabilidade_records
                        WHERE novo_status_bilhete IS NOT NULL AND novo_status_bilhete != ''
                        GROUP BY codigo_externo
                    ) ultimo ON pr.codigo_externo = ultimo.codigo_externo AND pr.id = ultimo.max_id
                )
                SELECT 
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
                    -- Usar o novo_status_bilhete mais recente não-nulo
                    COALESCE(usb.ultimo_novo_status_bilhete, pr.novo_status_bilhete, '') AS novo_status_bilhete,
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
                    ro.iccid AS ro_iccid,

                    -- Contadores de classificação (do status filtrado, não de todos os registros)
                    COALESCE(cc.total_classificacoes, 0) as total_classificacoes,
                    CASE WHEN COALESCE(cc.total_classificacoes, 0) > 1 THEN 'SIM' ELSE 'NAO' END AS houve_reclassificacao

                FROM portabilidade_records pr
                LEFT JOIN contagem_classificacoes cc ON pr.codigo_externo = cc.codigo_externo
                LEFT JOIN ultimo_status_bilhete usb ON pr.codigo_externo = usb.codigo_externo
                LEFT JOIN base_coverte_prop bc ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                    TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
                )
                LEFT JOIN relatorio_objetos ro ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), CAST(pr.codigo_externo AS TEXT), '')) = 
                    TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
                )
                WHERE 
                    -- Filtro: incluir TODAS as linhas com status "Erro no Aprovisionamento" (várias ordens por codigo_externo)
                    (
                        pr.status_ordem = 'Erro no Aprovisionamento'
                        OR pr.status_bilhete = 'Erro no Aprovisionamento'
                    )
                    -- Filtro: últimos 180 dias
                    AND (
                        (bc.data_venda IS NULL)
                        OR (COALESCE(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10), '9999-12-31')) """ + filtro_data_sql + """
                    )
                    -- EXIBIR NO RELATORIO FINAL: excluir APENAS Rejeição do Cliente via SMS em ambos motivos + Erro
                    AND NOT (
                        LOWER(COALESCE(pr.motivo_recusa,'')) LIKE '%rejei%cliente%sms%'
                        AND LOWER(COALESCE(pr.motivo_cancelamento,'')) LIKE '%rejei%cliente%sms%'
                        AND pr.status_ordem = 'Erro no Aprovisionamento'
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
                LIMIT 15000
                """
            else:
                # Fallback: usar apenas portabilidade_records
                query = """
                SELECT
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
                WHERE (status_ordem = 'Erro no Aprovisionamento'
                   OR status_bilhete = 'Erro no Aprovisionamento')
                   AND NOT (
                       LOWER(COALESCE(motivo_recusa,'')) LIKE '%rejei%cliente%sms%'
                       AND LOWER(COALESCE(motivo_cancelamento,'')) LIKE '%rejei%cliente%sms%'
                       AND status_ordem = 'Erro no Aprovisionamento'
                   )
                ORDER BY data_inicial_processamento DESC
                LIMIT 15000
                """

            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("\n⚠ Nenhum registro com erro no aprovisionamento encontrado!")
        return
    
    # Não deduplicar por codigo_externo: o relatório deve refletir TODAS as linhas com Erro no Aprovisionamento
    # (mesmo codigo_externo pode ter vários numero_acesso/numero_ordem com esse status)
    print(f"    >> {len(rows)} registros para processamento")
    
    # [2.1] Validar registros usando tabela portabilidade_processamento
    print("[2.1] Validando registros com tabela portabilidade_processamento...")
    try:
        # Converter rows para lista de dicionários
        registros_para_validar = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            registros_para_validar.append(row_dict)
        
        # Filtrar registros válidos
        registros_validos, registros_invalidos = filtrar_registros_validos(
            db_manager, registros_para_validar
        )
        
        # Estatísticas de validação
        stats_validacao = obter_estatisticas_validacao(db_manager)
        print(f"    >> {len(registros_validos)} registros válidos para processamento")
        print(f"    >> {len(registros_invalidos)} registros inválidos (serão ignorados)")
        if stats_validacao['total_registros'] > 0:
            print(f"    >> Estatísticas da tabela portabilidade_processamento:")
            print(f"       - Total: {stats_validacao['total_registros']}")
            print(f"       - Válidos: {stats_validacao['validos']}")
            print(f"       - Com conflito: {stats_validacao['com_conflito']}")
            print(f"       - Com cancelamento: {stats_validacao['com_cancelamento']}")
        
        # Reconstruir rows apenas com registros válidos
        rows_validos = []
        for registro in registros_validos:
            row_reconstruido = [registro.get(col, None) for col in columns]
            rows_validos.append(row_reconstruido)
        
        rows = rows_validos
        print(f"    >> Processando {len(rows)} registros válidos")
    except Exception as e:
        logger.warning(f"Erro ao validar registros (continuando sem validação): {e}")
        print(f"    >> Aviso: Validação não pôde ser executada, processando todos os registros")
    
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
    
    # [4] Converter para PortabilidadeRecord; filtrar por status Erro E histórico de entrega (regra unificada)
    print("[4] Processando registros (status Erro no Aprovisionamento + histórico de entrega)...")
    from src.models.portabilidade import PortabilidadeRecord
    
    erros_aprovisionamento = []  # lista de (record, record_dict) para escrever ro_* no arquivo
    results_map = {}  # Simular results_map vazio para homologação
    
    total_rows = len(rows)
    with ProgressBar(
        total=total_rows,
        desc="Processando erros de aprovisionamento",
        unit="registros",
        logger=logger,
        log_interval_pct=10.0
    ) as pbar:
        for row_idx, row in enumerate(rows, 1):
            record_dict = dict(zip(columns, row))
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
                pbar.update(1)
                continue
            
            # Verificar Status da ordem: deve ser "Erro no Aprovisionamento"
            status_ordem_valido = False
            if record.status_ordem:
                status_ordem_str = str(record.status_ordem.value if hasattr(record.status_ordem, 'value') else record.status_ordem)
                if 'Erro no Aprovisionamento' in status_ordem_str:
                    status_ordem_valido = True
            
            if not status_ordem_valido:
                pbar.update(1)
                continue
            
            # Regra unificada: só incluir se tiver ALGUM histórico de entrega (ro_* ou Relatório de Objetos)
            tem_historico_entrega = False
            if record_dict.get('ro_nu_pedido') or record_dict.get('ro_rastreio'):
                tem_historico_entrega = True
            if not tem_historico_entrega and record_dict.get('ro_status_entrega'):
                tem_historico_entrega = True
            if not tem_historico_entrega and record_dict.get('ro_ultima_ocorrencia'):
                tem_historico_entrega = True
            if not tem_historico_entrega and record_dict.get('ro_data_entrega'):
                tem_historico_entrega = True
            if not tem_historico_entrega and record_dict.get('ro_iccid'):
                iccid_str = str(record_dict.get('ro_iccid') or '').strip()
                if iccid_str and iccid_str.lower() != 'nan':
                    tem_historico_entrega = True
            if not tem_historico_entrega and objects_loader:
                obj_match = objects_loader.find_best_match(
                    codigo_externo=record.codigo_externo,
                    cpf=record.cpf
                )
                if obj_match:
                    tem_historico_entrega = True
            
            if not tem_historico_entrega:
                pbar.update(1)
                continue
            
            erros_aprovisionamento.append((record, record_dict))
            pbar.update(1)
            pbar.set_postfix(processados=len(erros_aprovisionamento))
            if row_idx % 100 == 0:
                logger.info(f"  Erro Aprovisionamento: {row_idx}/{total_rows} ({100*row_idx/total_rows:.1f}%) | erros: {len(erros_aprovisionamento)} | {datetime.now().strftime('%H:%M:%S')}")
    
    # Deduplicação por (cpf, numero_acesso): manter apenas o mais recente (já vem ordenado pela query)
    vistos_cpf_num = set()
    erros_dedup = []
    for item in erros_aprovisionamento:
        rec = item[0]
        chave = (str(rec.cpf or '').strip(), str(rec.numero_acesso or '').strip())
        if chave in vistos_cpf_num:
            continue
        vistos_cpf_num.add(chave)
        erros_dedup.append(item)
    if len(erros_aprovisionamento) != len(erros_dedup):
        print(f"    >> Deduplicação (cpf + numero_acesso): {len(erros_aprovisionamento)} → {len(erros_dedup)} registros")
    erros_aprovisionamento = erros_dedup

    print(f"    >> {len(erros_aprovisionamento)} registros com erro no aprovisionamento e histórico de entrega")
    
    # Cabeçalhos do arquivo (mesmo quando vazio, para o processar_completo encontrar o arquivo)
    HEADERS_ERRO_APROV = [
        'Cpf', 'Número de acesso', 'Número da ordem', 'Código externo', 'ICCID', 'ToutBox',
        'Número do bilhete', 'Status do bilhete', 'Operadora doadora', 'Data da portabilidade',
        'Motivo da recusa', 'Motivo do cancelamento', 'Último bilhete de portabilidade?',
        'Status da ordem', 'Preço da ordem', 'Data da conclusão da ordem', 'Motivo de não ter sido consultado',
        'Motivo de não ter sido cancelado', 'Motivo de não ter sido aberto', 'Motivo de não ter sido reagendado',
        'Novo status do bilhete', 'Nova data da portabilidade', 'Responsável pelo processamento',
        'Data inicial do processamento', 'Data final do processamento', 'Registro válido?',
        'Ajustes registro', 'Número de acesso válido?', 'Ajustes número de acesso',
        'Status da entrega', 'Data da entrega', 'Parâmetro de Identificação',
        'Data Última Atualização Coleta', 'Tipo de Venda'
    ]

    if not erros_aprovisionamento:
        print("\n⚠ Nenhum registro com erro no aprovisionamento e histórico de entrega encontrado!")
        OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)
        df_vazio = pd.DataFrame(columns=HEADERS_ERRO_APROV)
        df_vazio.to_excel(OUTPUT_HOMOLOGACAO, index=False, engine='openpyxl')
        print(f"    >> Arquivo vazio gerado em: {OUTPUT_HOMOLOGACAO}")
        return

    # [5] Gerar arquivo de homologação (CSV depois XLSX, sem filtro "entregue" do CSVGenerator)
    print("[5] Gerando arquivo de homologação...")
    output_csv = Path("data/homologacao_erro_aprovisionamento_temp.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    def _safe_str(val, default=''):
        return str(val) if val is not None else default

    def _safe_date(val, default=''):
        if val is None:
            return default
        try:
            if isinstance(val, datetime):
                return val.strftime("%d/%m/%Y")
            return str(val)
        except (ValueError, TypeError, AttributeError):
            return default

    def _safe_enum(val, default=''):
        if val is None:
            return default
        try:
            return val.value if hasattr(val, 'value') else str(val)
        except (ValueError, TypeError, AttributeError):
            return default

    def _safe_bool(val, default=''):
        if val is None:
            return default
        return 'Sim' if val else 'Não'

    try:
        import csv
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(HEADERS_ERRO_APROV)
            for record, record_dict in erros_aprovisionamento:
                status_entrega = _safe_str(record_dict.get('ro_status_entrega') or record_dict.get('ro_ultima_ocorrencia'))
                data_entrega = record_dict.get('ro_data_entrega')
                if data_entrega is not None:
                    data_entrega = _safe_date(data_entrega)
                else:
                    data_entrega = ''
                iccid = _safe_str(record_dict.get('ro_iccid'))
                tipo_venda = 'Portabilidade' if (record.operadora_doadora or record.data_portabilidade) else 'Nova Linha'
                row = [
                    _safe_str(record.cpf),
                    _safe_str(record.numero_acesso),
                    _safe_str(record.numero_ordem),
                    _safe_str(record.codigo_externo),
                    iccid,
                    '',
                    _safe_str(record.numero_bilhete),
                    _safe_enum(record.status_bilhete),
                    _safe_str(record.operadora_doadora),
                    _safe_date(record.data_portabilidade),
                    _safe_str(record.motivo_recusa),
                    _safe_str(record.motivo_cancelamento),
                    _safe_bool(record.ultimo_bilhete),
                    _safe_enum(record.status_ordem),
                    _safe_str(record.preco_ordem),
                    _safe_date(record.data_final_processamento) if hasattr(record, 'data_final_processamento') else '',
                    _safe_str(record.motivo_nao_consultado) if hasattr(record, 'motivo_nao_consultado') else '',
                    _safe_str(record.motivo_nao_cancelado) if hasattr(record, 'motivo_nao_cancelado') else '',
                    _safe_str(record.motivo_nao_aberto) if hasattr(record, 'motivo_nao_aberto') else '',
                    _safe_str(record.motivo_nao_reagendado) if hasattr(record, 'motivo_nao_reagendado') else '',
                    _safe_str(record.novo_status_bilhete) if hasattr(record, 'novo_status_bilhete') else '',
                    _safe_date(record.nova_data_portabilidade) if hasattr(record, 'nova_data_portabilidade') else '',
                    _safe_str(record.responsavel_processamento) if hasattr(record, 'responsavel_processamento') else '',
                    _safe_date(record.data_inicial_processamento) if hasattr(record, 'data_inicial_processamento') else '',
                    _safe_date(record.data_final_processamento) if hasattr(record, 'data_final_processamento') else '',
                    _safe_bool(record.registro_valido) if hasattr(record, 'registro_valido') else '',
                    _safe_str(record.ajustes_registro) if hasattr(record, 'ajustes_registro') else '',
                    _safe_bool(record.numero_acesso_valido) if hasattr(record, 'numero_acesso_valido') else '',
                    _safe_str(record.ajustes_numero_acesso) if hasattr(record, 'ajustes_numero_acesso') else '',
                    status_entrega,
                    data_entrega,
                    '',
                    '',
                    tipo_venda
                ]
                # Sanitizar valores (None/NULL/NaN → string vazia)
                row = [sanitizar_valor(v) for v in row]
                writer.writerow(row)

        df = pd.read_csv(output_csv, delimiter=';', encoding='utf-8-sig')
        if OUTPUT_HOMOLOGACAO.exists():
            OUTPUT_HOMOLOGACAO.unlink()
        df.to_excel(OUTPUT_HOMOLOGACAO, index=False, engine='openpyxl')
        if output_csv.exists():
            output_csv.unlink()
        print(f"    >> Arquivo salvo em: {OUTPUT_HOMOLOGACAO}")
        print()
        print("=" * 70)
        print("ESTATÍSTICAS DE HOMOLOGAÇÃO")
        print("=" * 70)
        print(f"  Total de registros: {len(erros_aprovisionamento)}")
        print()
        print("=" * 70)
        print("HOMOLOGAÇÃO GERADA COM SUCESSO!")
        print("=" * 70)
    except Exception as e:
        logger.error(f"Erro ao gerar arquivo: {e}")
        print(f"\n✗ ERRO ao gerar arquivo de homologação: {e}")
        if output_csv.exists():
            print(f"    >> CSV temporário em: {output_csv}")

if __name__ == "__main__":
    main()
