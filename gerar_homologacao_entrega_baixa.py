"""
Script para gerar arquivo de homologação de Entrega/Baixa
Filtra vendas em que a situação mais recente de entrega está:
- Cancelada
- Com baixa
- Remetente e aguardando correios
- Extraviada

EXCLUSÃO: Não inclui vendas com rejeição SMS em portabilidade_records,
portabilidade_processamento ou base_unificada.

Mesmo modelo de cabeçalho do gerar_homologacao_wpp.py
"""
from pathlib import Path
from datetime import datetime, timedelta

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import pandas as pd
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
        logging.FileHandler('logs/homologacao_entrega_baixa.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Caminhos (mesmo padrão dos outros scripts de homologação)
DB_PATH = "data/portabilidade.db"
OUTPUT_HOMOLOGACAO = Path("data/homologacao_entrega_baixa.xlsx")
DIAS_LIMITE_HOMOLOGACAO = 90

# Padrões de status de entrega que indicam cancelada/baixa/remetente
STATUS_ENTREGA_FILTRO = [
    'cancelada', 'cancelado',
    'baixa', 'baixo',
    'remetente', 'aguardando correios',
    'extraviada', 'extraviado', 'extravio'
]


def _status_entrega_qualifica(status: str) -> bool:
    """Verifica se o status de entrega indica cancelada/baixa/remetente."""
    if not status:
        return False
    s = str(status).strip().lower()
    return any(p in s for p in STATUS_ENTREGA_FILTRO)


def _separar_numero_complemento(numero_raw, complemento_raw) -> tuple:
    """
    Garante que numero contenha apenas dígitos e complementos vão para complemento.
    Ex: numero='123 sala 101', complemento='bloco A' -> ('123', 'bloco A sala 101')
    """
    def safe(v):
        return str(v).strip() if v is not None and str(v).strip() else ''
    num_s = safe(numero_raw)
    comp_s = safe(complemento_raw)
    digitos = ''.join(c for c in num_s if c.isdigit())
    nao_digitos = ''.join(c for c in num_s if not c.isdigit()).strip(' -,;.')
    comp_final = comp_s
    if nao_digitos:
        comp_final = (comp_s + ' ' + nao_digitos).strip() if comp_s else nao_digitos
    return (digitos, comp_final)


def main():
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO - ENTREGA/BAIXA")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # [1] Conectar ao banco de dados
    print("[1] Conectando ao banco de dados...")
    from src.database.db_manager import DatabaseManager
    db_manager = DatabaseManager(DB_PATH)
    
    # [2] Buscar vendas com status de entrega cancelada/baixa/remetente
    print("[2] Buscando vendas com entrega cancelada/baixa/remetente...")
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
            # Verificar existência da view vw_base_unificada antes de consultar
            _conn_check = _sqlite3.connect(DB_V2_PATH)
            try:
                _view_ok = _conn_check.execute(
                    "SELECT name FROM sqlite_master WHERE type='view' AND name='vw_base_unificada'"
                ).fetchone() is not None
            finally:
                _conn_check.close()

            if not _view_ok:
                logger.error(
                    "[V2] View vw_base_unificada não encontrada em %s — fallback para legado",
                    DB_V2_PATH,
                )
                print("    >> [V2] ❌ View vw_base_unificada ausente no banco v2, usando fallback legado")
            else:
                print("    >> [V2] Banco v2 detectado, usando QueriesV2 como fonte primária...")
                queries_v2 = QueriesV2(DB_V2_PATH)
                registros_v2 = queries_v2.buscar_registros_entrega_baixa(dias_limite=DIAS_LIMITE_HOMOLOGACAO)
                if registros_v2:
                    columns = list(registros_v2[0].keys())
                    rows = [tuple(r[c] for c in columns) for r in registros_v2]
                    _usou_v2 = True
                    logger.info("[V2] ✅ %d registros obtidos via QueriesV2.buscar_registros_entrega_baixa()", len(rows))
                    print(f"    >> [V2] ✅ {len(rows)} registros obtidos via QueriesV2")
                else:
                    logger.warning("[V2] ⚠ QueriesV2.buscar_registros_entrega_baixa() retornou 0 registros — ATENÇÃO: V2 pode estar com dados desatualizados ou cache vazia. Usando fallback legado.")
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
        
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
            tem_base_coverte = 'base_coverte_prop' in tabelas_existentes
            tem_relatorio_objetos = 'relatorio_objetos' in tabelas_existentes
            tem_portabilidade_processamento = 'portabilidade_processamento' in tabelas_existentes
            tem_base_unificada = 'base_unificada' in tabelas_existentes
        
            if tem_base_coverte:
                print("    >> Usando base_coverte_prop" + (" + relatorio_objetos (mais recente por data de atualização)" if tem_relatorio_objetos else ""))
            elif tem_relatorio_objetos:
                print("    >> base_coverte_prop não encontrada; usando apenas relatorio_objetos (mais recente por updated_at)")
            else:
                print("    >> Nenhuma tabela base_coverte_prop ou relatorio_objetos disponível. Nada a gerar.")
                return
        
            # Relatório de objetos: usar apenas o registro mais recente por codigo_externo (por data de atualização)
            ro_mais_recente_cte = ""
            ro_join = ""
            ro_cols = "NULL AS ro_status, NULL AS ro_ultima_ocorrencia, NULL AS rastreio, NULL AS nu_pedido"
            if tem_relatorio_objetos:
                ro_mais_recente_cte = """
                ro_mais_recente AS (
                    SELECT ro.* FROM relatorio_objetos ro
                    INNER JOIN (
                        SELECT codigo_externo, MAX(COALESCE(updated_at, created_at, '')) AS max_at
                        FROM relatorio_objetos GROUP BY codigo_externo
                    ) t ON TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), '')) = TRIM(COALESCE(CAST(t.codigo_externo AS TEXT), ''))
                      AND COALESCE(ro.updated_at, ro.created_at, '') = t.max_at
                ),
                """
                ro_join = """
                LEFT JOIN ro_mais_recente ro ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                    TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
                )
                """
                ro_cols = "ro.status AS ro_status, ro.ultima_ocorrencia AS ro_ultima_ocorrencia, ro.rastreio AS rastreio, ro.nu_pedido AS nu_pedido"
        
            # Subquery para excluir códigos com rejeição SMS em portabilidade_records
            sub_rejeicao_pr = """
                NOT EXISTS (
                    SELECT 1 FROM portabilidade_records pr2
                    WHERE TRIM(COALESCE(CAST(pr2.codigo_externo AS TEXT), '')) = 
                          TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), ''))
                    AND (
                        (pr2.status_bilhete IS NOT NULL AND LOWER(pr2.status_bilhete) LIKE '%rejeicao sms%')
                        OR LOWER(COALESCE(pr2.motivo_recusa,'')) LIKE '%rejei%cliente%sms%'
                        OR LOWER(COALESCE(pr2.motivo_cancelamento,'')) LIKE '%rejei%cliente%sms%'
                    )
                )
            """
        
            # Subquery para excluir códigos com rejeição em portabilidade_processamento
            sub_rejeicao_pp = ""
            if tem_portabilidade_processamento:
                sub_rejeicao_pp = """
                AND NOT EXISTS (
                    SELECT 1 FROM portabilidade_processamento pp
                    WHERE TRIM(COALESCE(CAST(pp.id_proposta_isize AS TEXT), CAST(pp.codigo_externo AS TEXT), '')) = 
                          TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), ''))
                    AND (
                        (pp.MOTIVO_CONFLITO IS NOT NULL AND LOWER(pp.MOTIVO_CONFLITO) LIKE '%rejei%cliente%sms%')
                        OR (pp.MOTIVO_CANCELAMENTO IS NOT NULL AND LOWER(pp.MOTIVO_CANCELAMENTO) LIKE '%rejei%cliente%sms%')
                    )
                )
                """
        
            # Subquery para excluir códigos com rejeição em base_unificada (se tabela tiver as colunas)
            sub_rejeicao_bu = ""
            if tem_base_unificada:
                try:
                    cursor.execute("PRAGMA table_info(base_unificada)")
                    colunas_bu = [str(row[1]).lower() for row in cursor.fetchall()]
                    if all(c in colunas_bu for c in ['status_bilhete', 'motivo_recusa', 'motivo_cancelamento']):
                        sub_rejeicao_bu = """
                AND NOT EXISTS (
                    SELECT 1 FROM base_unificada bu
                    WHERE TRIM(COALESCE(CAST(bu.proposta_isize AS TEXT), CAST(bu.codigo_externo AS TEXT), '')) = 
                          TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), ''))
                    AND (
                        (bu.status_bilhete IS NOT NULL AND LOWER(bu.status_bilhete) LIKE '%rejeicao sms%')
                        OR LOWER(COALESCE(bu.motivo_recusa,'')) LIKE '%rejei%cliente%sms%'
                        OR LOWER(COALESCE(bu.motivo_cancelamento,'')) LIKE '%rejei%cliente%sms%'
                    )
                )
                """
                except Exception:
                    pass
        
            # Última situação de entrega: prioridade ro.ultima_ocorrencia > ro.status > status_correios/loggi/entrega_prevista
            cond_status_entrega = """
                (
                    (ro.ultima_ocorrencia IS NOT NULL AND ro.ultima_ocorrencia != ''
                     AND (LOWER(ro.ultima_ocorrencia) LIKE '%cancelad%' OR LOWER(ro.ultima_ocorrencia) LIKE '%baixa%'
                          OR LOWER(ro.ultima_ocorrencia) LIKE '%remetente%' OR LOWER(ro.ultima_ocorrencia) LIKE '%aguardando correios%'
                          OR LOWER(ro.ultima_ocorrencia) LIKE '%extravi%'))
                    OR (ro.status IS NOT NULL AND ro.status != ''
                        AND (LOWER(ro.status) LIKE '%cancelad%' OR LOWER(ro.status) LIKE '%baixa%'
                             OR LOWER(ro.status) LIKE '%remetente%' OR LOWER(ro.status) LIKE '%aguardando correios%'
                             OR LOWER(ro.status) LIKE '%extravi%'))
                    OR (LOWER(COALESCE(bc.status_correios,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%extravi%')
                    OR (LOWER(COALESCE(bc.status_loggi,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%extravi%')
                    OR (LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%extravi%')
                )
            """
        
            if not tem_relatorio_objetos:
                cond_status_entrega = """
                (
                    (LOWER(COALESCE(bc.status_correios,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_correios,'')) LIKE '%extravi%')
                    OR (LOWER(COALESCE(bc.status_loggi,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_loggi,'')) LIKE '%extravi%')
                    OR (LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%cancelad%' OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%baixa%'
                        OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%remetente%' OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%aguardando correios%'
                        OR LOWER(COALESCE(bc.status_entrega_prevista,'')) LIKE '%extravi%')
                )
                """
        
            if tem_base_coverte:
                with_clause = f"WITH {ro_mais_recente_cte.rstrip().rstrip(',').rstrip()}\n            " if ro_mais_recente_cte else ""
                query = f"""
                {with_clause}SELECT DISTINCT
                    COALESCE(bc.proposta_isize, bc.codigo_externo, '') AS codigo_externo,
                    COALESCE(bc.cpf, '') AS cpf,
                    bc.cliente_nome AS cliente_nome,
                    bc.telefone_portado AS telefone_portado,
                    bc.data_venda AS data_venda,
                    bc.data_conectada AS data_conectada,
                    bc.plano AS plano,
                    bc.endereco AS endereco,
                    bc.numero AS numero,
                    bc.complemento AS complemento,
                    bc.bairro AS bairro,
                    bc.cidade AS cidade,
                    bc.uf AS uf,
                    bc.cep AS cep,
                    bc.ponto_referencia AS ponto_referencia,
                    bc.crivo_vendas AS crivo_vendas,
                    COALESCE(bc.status_correios, bc.status_loggi, bc.status_entrega_prevista, '') AS status_entrega_coverte,
                    {ro_cols}
                FROM base_coverte_prop bc
                {ro_join}
                WHERE bc.proposta_isize IS NOT NULL 
                  AND TRIM(COALESCE(bc.proposta_isize, bc.codigo_externo, '')) != ''
                  AND {cond_status_entrega}
                  AND (
                    (bc.data_venda IS NULL)
                    OR (COALESCE(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10), '9999-12-31')) {filtro_data_sql}
                  )
                  AND {sub_rejeicao_pr}
                  {sub_rejeicao_pp}
                  {sub_rejeicao_bu}
                ORDER BY bc.data_venda DESC NULLS LAST, bc.proposta_isize DESC
                LIMIT 15000
                """
            else:
                # Sem base_coverte_prop: usar apenas relatorio_objetos (mais recente por updated_at)
                cond_ro = """
                (LOWER(COALESCE(ro.ultima_ocorrencia,'')) LIKE '%cancelad%' OR LOWER(COALESCE(ro.ultima_ocorrencia,'')) LIKE '%baixa%'
                 OR LOWER(COALESCE(ro.ultima_ocorrencia,'')) LIKE '%remetente%' OR LOWER(COALESCE(ro.ultima_ocorrencia,'')) LIKE '%aguardando correios%'
                 OR LOWER(COALESCE(ro.ultima_ocorrencia,'')) LIKE '%extravi%'
                 OR LOWER(COALESCE(ro.status,'')) LIKE '%cancelad%' OR LOWER(COALESCE(ro.status,'')) LIKE '%baixa%'
                 OR LOWER(COALESCE(ro.status,'')) LIKE '%remetente%' OR LOWER(COALESCE(ro.status,'')) LIKE '%aguardando correios%'
                 OR LOWER(COALESCE(ro.status,'')) LIKE '%extravi%')
                """
                sub_rej_ro = ""
                if tem_portabilidade_processamento:
                    sub_rej_ro = """
                  AND NOT EXISTS (
                    SELECT 1 FROM portabilidade_processamento pp
                    WHERE TRIM(COALESCE(CAST(pp.id_proposta_isize AS TEXT), CAST(pp.codigo_externo AS TEXT), '')) =
                          TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
                    AND (LOWER(COALESCE(pp.MOTIVO_CONFLITO,'')) LIKE '%rejei%cliente%sms%'
                         OR LOWER(COALESCE(pp.MOTIVO_CANCELAMENTO,'')) LIKE '%rejei%cliente%sms%')
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM portabilidade_records pr2
                    WHERE TRIM(COALESCE(CAST(pr2.codigo_externo AS TEXT), '')) = TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
                    AND (LOWER(COALESCE(pr2.status_bilhete,'')) LIKE '%rejeicao sms%'
                         OR LOWER(COALESCE(pr2.motivo_recusa,'')) LIKE '%rejei%cliente%sms%'
                         OR LOWER(COALESCE(pr2.motivo_cancelamento,'')) LIKE '%rejei%cliente%sms%')
                  )
                """
                query = f"""
                WITH ro_mais_recente AS (
                    SELECT ro.* FROM relatorio_objetos ro
                    INNER JOIN (
                        SELECT codigo_externo, MAX(COALESCE(updated_at, created_at, '')) AS max_at
                        FROM relatorio_objetos GROUP BY codigo_externo
                    ) t ON TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), '')) = TRIM(COALESCE(CAST(t.codigo_externo AS TEXT), ''))
                      AND COALESCE(ro.updated_at, ro.created_at, '') = t.max_at
                )
                SELECT DISTINCT
                    COALESCE(ro.codigo_externo, '') AS codigo_externo,
                    COALESCE(ro.documento, '') AS cpf,
                    COALESCE(ro.destinatario, '') AS cliente_nome,
                    '' AS telefone_portado,
                    ro.data_criacao_pedido AS data_venda,
                    ro.data_insercao AS data_conectada,
                    '' AS plano,
                    '' AS endereco,
                    '' AS numero,
                    '' AS complemento,
                    '' AS bairro,
                    COALESCE(ro.cidade_ultima_ocorrencia, ro.cidade, '') AS cidade,
                    COALESCE(ro.estado_ultima_ocorrencia, ro.uf, '') AS uf,
                    COALESCE(ro.cep, '') AS cep,
                    '' AS ponto_referencia,
                    '' AS crivo_vendas,
                    COALESCE(ro.status, ro.ultima_ocorrencia, '') AS status_entrega_coverte,
                    ro.status AS ro_status,
                    ro.ultima_ocorrencia AS ro_ultima_ocorrencia,
                    ro.rastreio AS rastreio,
                    ro.nu_pedido AS nu_pedido
                FROM ro_mais_recente ro
                WHERE ro.codigo_externo IS NOT NULL AND TRIM(COALESCE(ro.codigo_externo,'')) != ''
                  AND {cond_ro}
                  AND (ro.data_insercao IS NULL OR COALESCE(SUBSTR(TRIM(CAST(ro.data_insercao AS TEXT)), 1, 10), '9999-12-31') {filtro_data_sql})
                  {sub_rej_ro}
                ORDER BY ro.data_insercao DESC NULLS LAST, ro.codigo_externo DESC
                LIMIT 15000
                """
        
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
    
    print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("\n⚠ Nenhum registro de entrega cancelada/baixa/remetente encontrado!")
        return
    
    # [3] Montar dados no formato do cabeçalho WPP
    print("[3] Montando dados no formato de homologação...")
    
    def safe_str(v, default=''):
        return str(v).strip() if v is not None and str(v).strip() else default
    
    def safe_date(v):
        if not v:
            return ''
        try:
            s = str(v).strip()
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%d/%m/%Y %H:%M:%S']:
                try:
                    return datetime.strptime(s[:19] if len(s) > 19 else s, fmt).strftime('%d/%m/%Y')
                except ValueError:
                    continue
            return s[:10] if len(s) >= 10 else s
        except Exception:
            pass
        return ''
    
    def formatar_link_rastreio(nu_pedido, codigo_externo):
        if nu_pedido and str(nu_pedido).strip():
            return f"https://tim.trakin.co/o/{str(nu_pedido).strip()}"
        codigo_limpo = str(codigo_externo).strip().lstrip('0') or '0'
        return f"https://tim.trakin.co/o/26-{codigo_limpo.zfill(8)}"
    
    homologacao_data = []
    vistos = set()  # Evitar duplicadas por codigo_externo
    total_rows = len(rows)
    for row_idx, row in enumerate(rows, 1):
        d = dict(zip(columns, row))
        codigo = safe_str(d.get('codigo_externo'))
        if not codigo:
            continue
        if codigo in vistos:
            continue  # Manter apenas o mais recente (já ordenado por data_venda DESC)
        vistos.add(codigo)

        status_coverte = safe_str(d.get('status_entrega_coverte'))
        ro_status = safe_str(d.get('ro_status'))
        ro_ultima = safe_str(d.get('ro_ultima_ocorrencia'))
        
        # Usar a situação mais recente de entrega (prioridade ro > coverte)
        status_entrega = ro_ultima or ro_status or status_coverte or ''
        
        numero_limpo, complemento_limpo = _separar_numero_complemento(
            d.get('numero'), d.get('complemento')
        )
        
        homologacao_data.append({
            'Proposta_iSize': codigo,
            'Cpf': safe_str(d.get('cpf')),
            'NomeCliente': safe_str(d.get('cliente_nome')),
            'Telefone_Contato': safe_str(d.get('telefone_portado')),
            'Endereco': safe_str(d.get('endereco')),
            'Numero': numero_limpo,
            'Complemento': complemento_limpo,
            'Bairro': safe_str(d.get('bairro')),
            'Cidade': safe_str(d.get('cidade')),
            'UF': safe_str(d.get('uf')),
            'Cep': safe_str(d.get('cep')),
            'Ponto_Referencia': safe_str(d.get('ponto_referencia')),
            'Cod_Rastreio': formatar_link_rastreio(d.get('nu_pedido'), codigo),
            'Data_Venda': safe_date(d.get('data_venda')),
            'Data_Conectada': safe_date(d.get('data_conectada')),
            'Tipo_Comunicacao': '',
            'Status_Disparo': 'FALSE',
            'DataHora_Disparo': '',
            'Template_Triggers': '',
            'O_Que_Aconteceu': f'Entrega: {status_entrega[:80]}' if status_entrega else 'Entrega cancelada/baixa/remetente',
            'Tentativas': '0',
            'Total_Classificacoes': '1',
            'Houve_Reclassificacao': 'NAO',
            'Acao_Realizar': '',
            'Status_Entrega': status_entrega
        })
        if row_idx % 100 == 0:
            logger.info(f"  Entrega/Baixa: {len(homologacao_data)} únicos (processados {row_idx}/{total_rows}) | {datetime.now().strftime('%H:%M:%S')}")
    
    duplicados_removidos = total_rows - len(homologacao_data)
    if duplicados_removidos > 0:
        print(f"    >> Duplicados removidos por codigo_externo: {duplicados_removidos}")

    # Deduplicação por (cpf, telefone): manter apenas o mais recente (já vem ordenado pela query)
    vistos_cpf_tel = set()
    homologacao_dedup = []
    for row_data in homologacao_data:
        chave = (str(row_data.get('Cpf') or '').strip(), str(row_data.get('Telefone_Contato') or '').strip())
        if chave in vistos_cpf_tel:
            continue
        vistos_cpf_tel.add(chave)
        homologacao_dedup.append(row_data)
    if len(homologacao_data) != len(homologacao_dedup):
        print(f"    >> Deduplicação (cpf + telefone): {len(homologacao_data)} → {len(homologacao_dedup)} registros")
    homologacao_data = homologacao_dedup
    
    # [4] Gerar arquivo XLSX
    print("[4] Gerando arquivo de homologação...")
    
    OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)
    
    colunas_principais = [
        'Proposta_iSize', 'Cpf', 'NomeCliente', 'Telefone_Contato',
        'Endereco', 'Numero', 'Complemento', 'Bairro', 'Cidade', 'UF', 'Cep', 'Ponto_Referencia',
        'Cod_Rastreio', 'Data_Venda', 'Data_Conectada', 'Tipo_Comunicacao',
        'Status_Disparo', 'DataHora_Disparo'
    ]
    colunas_homologacao = [
        'Template_Triggers', 'O_Que_Aconteceu', 'Tentativas', 'Total_Classificacoes',
        'Houve_Reclassificacao', 'Acao_Realizar', 'Status_Entrega'
    ]
    fieldnames = colunas_principais + colunas_homologacao
    
    # Sanitizar valores (None/NULL/NaN → string vazia)
    for row_data in homologacao_data:
        for key in row_data:
            if isinstance(row_data[key], str):
                row_data[key] = sanitizar_valor(row_data[key])
            elif row_data[key] is None:
                row_data[key] = ''

    df = pd.DataFrame(homologacao_data, columns=fieldnames)
    output_final = OUTPUT_HOMOLOGACAO
    try:
        df.to_excel(OUTPUT_HOMOLOGACAO, index=False, engine='openpyxl', sheet_name='Entrega_Baixa')
    except PermissionError:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_final = OUTPUT_HOMOLOGACAO.parent / f"homologacao_entrega_baixa_{timestamp}.xlsx"
        print(f"    >> Arquivo original está aberto, salvando como: {output_final.name}")
        df.to_excel(output_final, index=False, engine='openpyxl', sheet_name='Entrega_Baixa')
    
    print(f"    >> Arquivo salvo em: {output_final}")
    print()
    print("=" * 70)
    print("ESTATÍSTICAS DE HOMOLOGAÇÃO")
    print("=" * 70)
    print(f"  Total de registros: {len(homologacao_data)}")
    print()
    print("=" * 70)
    print("HOMOLOGAÇÃO ENTREGA/BAIXA GERADA COM SUCESSO!")
    print("=" * 70)


if __name__ == "__main__":
    main()
