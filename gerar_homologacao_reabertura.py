"""
Script para gerar arquivo de homologação de Reabertura
Filtra registros cancelados e agrupa por CPF
"""
from pathlib import Path
from datetime import datetime, timedelta

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem
from src.utils.csv_generator import CSVGenerator
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
        logging.FileHandler('logs/homologacao_reabertura.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos
DB_PATH = "data/portabilidade.db"
# BUG 6 FIX: Requisito 2 especifica 180 dias para reabertura, não 90
DIAS_LIMITE_HOMOLOGACAO = 180
OUTPUT_HOMOLOGACAO = Path("data/homologacao_reabertura.xlsx")
OUTPUT_TEMP = Path("data/homologacao_reabertura_temp.xlsx")
# Base analítica agora vem do banco (base_coverte_prop) - não precisa mais de arquivo CSV
BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - REABERTURA")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar registros de reabertura usando todas as tabelas disponíveis no portabilidade.db
    print("[2] Buscando registros de reabertura (usando todas as tabelas disponíveis no portabilidade.db)...")
    data_limite = (datetime.now() - timedelta(days=DIAS_LIMITE_HOMOLOGACAO)).strftime('%Y-%m-%d')
    filtro_data_sql = ">= '" + data_limite + "'"
    print(f"    >> Filtro: últimos {DIAS_LIMITE_HOMOLOGACAO} dias (a partir de {data_limite}) | Ordenação: mais recente primeiro")

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
                registros_v2 = queries_v2.buscar_registros_reabertura(dias_limite=DIAS_LIMITE_HOMOLOGACAO)
                if registros_v2:
                    columns = list(registros_v2[0].keys())
                    rows = [tuple(r[c] for c in columns) for r in registros_v2]
                    _usou_v2 = True
                    logger.info("[V2] ✅ %d registros obtidos via QueriesV2.buscar_registros_reabertura()", len(rows))
                    print(f"    >> [V2] ✅ {len(rows)} registros obtidos via QueriesV2")
                else:
                    logger.warning("[V2] ⚠ QueriesV2.buscar_registros_reabertura() retornou 0 registros — ATENÇÃO: V2 pode estar com dados desatualizados ou cache vazia. Usando fallback legado.")
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

            # Query sincronizada usando todas as tabelas disponíveis: base_coverte_prop + portabilidade_records + relatorio_objetos
            # USANDO O REGISTRO MAIS RECENTE POR codigo_externo (independente do status)
            # Filtro de status aplicado DEPOIS de pegar o registro mais recente
            if tem_base_coverte:
                query = """
                WITH registros_mais_recentes AS (
                    -- Subquery para pegar o registro MAIS RECENTE por codigo_externo (SEM filtro de status)
                    -- Isso garante que pegamos o status ATUAL, não um status antigo
                    SELECT 
                        codigo_externo,
                        MAX(id) as max_id
                    FROM portabilidade_records
                    WHERE codigo_externo IS NOT NULL AND codigo_externo != ''
                    GROUP BY codigo_externo
                ),
                -- CTE separada para contar classificações relevantes (com o status de reabertura)
                contagem_classificacoes AS (
                    SELECT 
                        codigo_externo,
                        COUNT(*) as total_classificacoes
                    FROM portabilidade_records
                    WHERE (
                        status_bilhete = 'Portabilidade Cancelada'
                        OR (motivo_cancelamento IS NOT NULL AND motivo_cancelamento != '' AND motivo_cancelamento != 'NULL')
                    )
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

                    -- Status de portabilidade (portabilidade_records) - usar o mais recente
                    COALESCE(pr.status_bilhete, '') AS status_bilhete,
                    COALESCE(pr.status_ordem, '') AS status_ordem,
                    COALESCE(pr.operadora_doadora, '') AS operadora_doadora,

                    -- novo_status_bilhete - usar o mais recente não-nulo
                    COALESCE(usb.ultimo_novo_status_bilhete, pr.novo_status_bilhete, '') AS novo_status_bilhete,

                    -- Datas
                    COALESCE(pr.data_portabilidade, '') AS data_portabilidade,
                    bc.data_venda AS data_venda,

                    -- Motivos (portabilidade_records)
                    COALESCE(pr.motivo_cancelamento, '') AS motivo_cancelamento,
                    COALESCE(pr.motivo_recusa, '') AS motivo_recusa,

                    -- Preço (portabilidade_records)
                    COALESCE(pr.preco_ordem, '') AS preco_ordem,

                    -- Dados adicionais de base_coverte_prop
                    bc.cliente_nome,
                    bc.telefone_portado,
                    bc.numero_linha,
                    bc.plano,
                    bc.crivo_vendas,
                    bc.bluechip_status,

                    -- Dados de logística (relatorio_objetos)
                    ro.nu_pedido AS ro_nu_pedido,
                    ro.rastreio AS ro_rastreio,
                    ro.status AS ro_status_entrega,
                    ro.transportadora AS ro_transportadora,

                    -- Contadores de classificação (do status filtrado, não de todos os registros)
                    COALESCE(cc.total_classificacoes, 0) as total_classificacoes,
                    CASE WHEN COALESCE(cc.total_classificacoes, 0) > 1 THEN 'SIM' ELSE 'NAO' END AS houve_reclassificacao

                FROM portabilidade_records pr
                INNER JOIN registros_mais_recentes rmr ON (
                    pr.codigo_externo = rmr.codigo_externo AND pr.id = rmr.max_id
                )
                LEFT JOIN contagem_classificacoes cc ON pr.codigo_externo = cc.codigo_externo
                LEFT JOIN ultimo_status_bilhete usb ON pr.codigo_externo = usb.codigo_externo
                LEFT JOIN base_coverte_prop bc ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                    TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
                )
                LEFT JOIN relatorio_objetos ro ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                    TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
                )
                WHERE 
                    -- Filtro de reabertura: O status MAIS RECENTE deve ser Portabilidade Cancelada
                    -- ou ter motivo de cancelamento. Se foi atualizado para outro status, NÃO aparece aqui
                    (
                        pr.status_bilhete = 'Portabilidade Cancelada'
                        OR (pr.motivo_cancelamento IS NOT NULL AND pr.motivo_cancelamento != '' AND pr.motivo_cancelamento != 'NULL')
                    )
                    -- Filtro: últimos 180 dias (data_venda ou data_inicial_processamento)
                    AND (
                        (bc.data_venda IS NULL)
                        OR (COALESCE(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10), '9999-12-31')) """ + filtro_data_sql + """
                    )
                    -- Não exibir vendas com status_bilhete like rejeicao sms
                    AND (pr.status_bilhete IS NULL OR pr.status_bilhete NOT LIKE '%rejeicao sms%')
                    -- Garantir que há pelo menos um identificador válido
                    AND (
                        bc.proposta_isize IS NOT NULL 
                        OR bc.codigo_externo IS NOT NULL
                        OR pr.codigo_externo IS NOT NULL
                        OR ro.codigo_externo IS NOT NULL
                    )
                ORDER BY 
                    -- Priorizar vendas mais recentes (incluindo 2026)
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
                    data_portabilidade, motivo_cancelamento, motivo_recusa,
                    preco_ordem,
                    NULL AS data_venda,
                    NULL AS cliente_nome,
                    NULL AS telefone_portado,
                    NULL AS numero_linha,
                    NULL AS plano,
                    NULL AS crivo_vendas,
                    NULL AS bluechip_status,
                    NULL AS ro_nu_pedido,
                    NULL AS ro_rastreio,
                    NULL AS ro_status_entrega,
                    NULL AS ro_transportadora
                FROM portabilidade_records
                WHERE (status_bilhete = 'Portabilidade Cancelada'
                   OR motivo_cancelamento IS NOT NULL
                   OR motivo_cancelamento != '')
                   AND (status_bilhete IS NULL OR status_bilhete NOT LIKE '%rejeicao sms%')
                   AND (data_inicial_processamento IS NULL OR date(data_inicial_processamento) """ + filtro_data_sql + """)
                ORDER BY data_inicial_processamento DESC
                LIMIT 1000
                """

            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("\n⚠ Nenhum registro cancelado encontrado!")
        return
    
    # [2.0.1] DEDUPLICAÇÃO: Remover duplicatas por codigo_externo (manter o primeiro)
    print("[2.0.1] Removendo duplicatas por codigo_externo...")
    registros_unicos = {}
    duplicatas_removidas = 0
    for row in rows:
        row_dict = dict(zip(columns, row))
        codigo_externo = str(row_dict.get('codigo_externo', '')).strip()
        if codigo_externo and codigo_externo not in registros_unicos:
            registros_unicos[codigo_externo] = row
        elif codigo_externo:
            duplicatas_removidas += 1
    
    rows = list(registros_unicos.values())
    if duplicatas_removidas > 0:
        print(f"    >> {duplicatas_removidas} duplicatas removidas")
    print(f"    >> {len(rows)} registros únicos para processamento")
    
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
    
    # [3] Converter para PortabilidadeRecord
    print("[3] Processando registros...")
    from src.models.portabilidade import PortabilidadeRecord
    
    reabertura = []
    results_map = {}  # Simular results_map vazio para homologação
    
    total_rows = len(rows)
    with ProgressBar(
        total=total_rows,
        desc="Processando reaberturas",
        unit="registros",
        logger=logger,
        log_interval_pct=10.0
    ) as pbar:
        for row_idx, row in enumerate(rows, 1):
            record_dict = dict(zip(columns, row))
            # Sanitizar valores (None/NULL/NaN → string vazia)
            for k in record_dict:
                record_dict[k] = sanitizar_valor(record_dict[k])
            try:
                # Normalizar CPF (remover formatação)
                cpf = str(record_dict.get('cpf', '')).strip()
                cpf = cpf.replace('.', '').replace('-', '').replace('/', '').replace(' ', '')
                if len(cpf) != 11 or not cpf.isdigit():
                    cpf = ''
                
                # Processar data_portabilidade
                data_portabilidade = None
                if record_dict.get('data_portabilidade'):
                    try:
                        data_portabilidade = datetime.fromisoformat(str(record_dict['data_portabilidade']))
                    except Exception:
                        try:
                            data_str = str(record_dict['data_portabilidade']).strip()
                            if len(data_str) >= 10:
                                data_portabilidade = datetime.strptime(data_str[:10], '%Y-%m-%d')
                        except Exception:
                            pass
                
                record = PortabilidadeRecord(
                    cpf=cpf,
                    numero_acesso=str(record_dict.get('numero_acesso', '')).strip(),
                    numero_ordem=str(record_dict.get('numero_ordem', '')).strip(),
                    codigo_externo=str(record_dict.get('codigo_externo', '')).strip(),
                    status_bilhete=PortabilidadeStatus(record_dict['status_bilhete']) if record_dict.get('status_bilhete') and str(record_dict['status_bilhete']).strip() else None,
                    status_ordem=StatusOrdem(record_dict['status_ordem']) if record_dict.get('status_ordem') and str(record_dict['status_ordem']).strip() else None,
                    operadora_doadora=str(record_dict.get('operadora_doadora', '')).strip() if record_dict.get('operadora_doadora') else None,
                    data_portabilidade=data_portabilidade,
                    motivo_cancelamento=str(record_dict.get('motivo_cancelamento', '')).strip() if record_dict.get('motivo_cancelamento') else None,
                    motivo_recusa=str(record_dict.get('motivo_recusa', '')).strip() if record_dict.get('motivo_recusa') else None,
                    preco_ordem=str(record_dict.get('preco_ordem', '')).strip() if record_dict.get('preco_ordem') else None
                )
                reabertura.append(record)
                pbar.update(1)
                pbar.set_postfix(processados=len(reabertura))
                if row_idx % 100 == 0:
                    logger.info(f"  Reabertura: {row_idx}/{total_rows} ({100*row_idx/total_rows:.1f}%) | reaberturas: {len(reabertura)} | {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                logger.error(f"Erro ao criar record: {e} - Dados: {record_dict}")
                pbar.update(1)
                continue
    
    # Deduplicação por (cpf, numero_acesso): manter apenas o mais recente (já vem ordenado pela query)
    vistos_cpf_num = set()
    reabertura_dedup = []
    for rec in reabertura:
        chave = (str(rec.cpf or '').strip(), str(rec.numero_acesso or '').strip())
        if chave in vistos_cpf_num:
            continue
        vistos_cpf_num.add(chave)
        reabertura_dedup.append(rec)
    if len(reabertura) != len(reabertura_dedup):
        print(f"    >> Deduplicação (cpf + numero_acesso): {len(reabertura)} → {len(reabertura_dedup)} registros")
    reabertura = reabertura_dedup

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
        base_analitica_loader,
        db_manager
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

