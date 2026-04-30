"""
Queries otimizadas para o novo schema v2.

Fornece funções de consulta que usam as views vw_*_corrente e
vw_base_unificada do novo banco de dados normalizado.

Cada método retorna uma lista de dicts com os mesmos nomes de colunas
que os geradores de homologação existentes esperam, permitindo uso
como substituto drop-in das queries antigas.
"""
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class QueriesV2:
    """Consultas otimizadas usando as views do schema v2."""

    def __init__(self, db_v2):
        """
        Inicializa com um DatabaseManagerV2 ou caminho de banco.

        Args:
            db_v2: Instância de DatabaseManagerV2 ou caminho (str/Path) para o banco v2.
        """
        if hasattr(db_v2, 'db_path'):
            self.db_path = db_v2.db_path
        else:
            self.db_path = str(db_v2)

    @contextmanager
    def _conn(self):
        """Context manager de conexão SQLite com row_factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            yield conn
        finally:
            conn.close()

    def _rows_to_dicts(self, rows) -> List[Dict[str, Any]]:
        """Converte lista de sqlite3.Row em lista de dicts."""
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Verificação de disponibilidade das views
    # ------------------------------------------------------------------

    def _view_exists(self, conn: sqlite3.Connection, view_name: str) -> bool:
        """Verifica se uma view existe no banco."""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            (view_name,)
        )
        return cursor.fetchone() is not None

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        """Verifica se uma tabela existe no banco."""
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None

    # ------------------------------------------------------------------
    # Helper: Rejeição SMS filter subclauses
    # ------------------------------------------------------------------

    def _rejeicao_sms_filter(
        self, conn: sqlite3.Connection, main_alias: str
    ) -> str:
        """
        Retorna subcláusulas NOT EXISTS para excluir registros com
        rejeição SMS em vw_portabilidade_tim_corrente e
        vw_consulta_siebel_corrente.

        Args:
            conn: Conexão SQLite ativa.
            main_alias: Alias da tabela principal (ex: 'bu', 'cs').

        Returns:
            String SQL com as subcláusulas AND NOT EXISTS, ou string
            vazia se as views não existem.
        """
        clauses = []

        if self._view_exists(conn, 'vw_portabilidade_tim_corrente'):
            clauses.append(f"""
                AND NOT EXISTS (
                    SELECT 1 FROM vw_portabilidade_tim_corrente pt
                    WHERE pt.proposta_isize = {main_alias}.proposta_isize
                    AND (
                        LOWER(COALESCE(pt.motivo_conflito, ''))
                            LIKE '%rejei%cliente%sms%'
                        OR LOWER(COALESCE(pt.motivo_cancelamento, ''))
                            LIKE '%rejei%cliente%sms%'
                    )
                )""")

        if self._view_exists(conn, 'vw_consulta_siebel_corrente'):
            clauses.append(f"""
                AND NOT EXISTS (
                    SELECT 1 FROM vw_consulta_siebel_corrente cs2
                    WHERE cs2.proposta_isize = {main_alias}.proposta_isize
                    AND (
                        LOWER(COALESCE(cs2.status_bilhete, ''))
                            LIKE '%rejeicao sms%'
                        OR LOWER(COALESCE(cs2.motivo_recusa, ''))
                            LIKE '%rejei%cliente%sms%'
                        OR LOWER(COALESCE(cs2.motivo_cancelamento, ''))
                            LIKE '%rejei%cliente%sms%'
                    )
                )""")

        return '\n'.join(clauses)

    # ------------------------------------------------------------------
    # 1. Registros para homologação WPP
    # ------------------------------------------------------------------

    def buscar_registros_wpp(
        self, dias_limite: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Busca registros para homologação WPP.

        Usa views correntes do V2 diretamente via vw_base_unificada
        (VIEW que faz JOINs de todas as views correntes).
        Retorna todos os registros de portabilidade dentro do
        limite de dias, com os campos que gerar_homologacao_wpp.py espera.
        Sem filtro de regra de decisão — parametrização feita no gerador.

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com colunas compatíveis com o gerador WPP.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_base_unificada'):
                logger.warning(
                    "View vw_base_unificada não encontrada no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'bu')

            query = f"""
                SELECT
                    bu.proposta_isize       AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        ''
                    ) AS cpf,
                    bu.nome_cliente,
                    bu.telefone_portabilidade AS numero_acesso,
                    bu.numero_linha          AS numero_temporario,
                    bu.numero_ordem,
                    bu.data_venda,
                    bu.produto,
                    bu.plano,
                    bu.status_venda,
                    bu.portabilidade_status,
                    bu.status_tim,
                    bu.data_ativacao_tim,
                    bu.status_logistica,
                    bu.rastreio_logistica    AS rastreio,
                    bu.data_entrega,
                    bu.data_gross,
                    bu.classificacao_cr,
                    bu.resultado_gross,
                    bu.status_pedido,
                    bu.detalhe_status,
                    bu.status_bilhete,
                    bu.status_ordem,
                    bu.bluechip_status,
                    bu.pedido_bluechip,
                    bu.regra_id,
                    bu.acao_a_realizar,
                    bu.tipo_mensagem,
                    c.endereco,
                    c.numero,
                    c.complemento,
                    c.bairro,
                    c.cidade,
                    c.uf,
                    c.cep,
                    c.ponto_referencia,
                    c.ddd_1,
                    c.telefone_1,
                    c.email,
                    p.data_venda             AS data_venda_proposta,
                    p.nome_vendedor,
                    p.nome_equipe
                FROM vw_base_unificada bu
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = bu.proposta_isize
                LEFT JOIN vw_clientes_corrente c
                    ON c.cpf = bu.cpf
                WHERE (
                      p.data_venda IS NULL
                      OR p.data_venda >= :data_limite
                  )
                  {rejeicao_filter}
                ORDER BY p.data_venda DESC
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_wpp: %d registros encontrados "
                    "(últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error("Erro em buscar_registros_wpp: %s", e)
                return []

    # ------------------------------------------------------------------
    # 2. Registros em aprovisionamento
    # ------------------------------------------------------------------

    def buscar_registros_aprovisionamento(
        self, dias_limite: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Busca registros em aprovisionamento.

        Usa vw_consulta_siebel_corrente + vw_base_unificada.
        Retorna registros com status_ordem = 'Em Aprovisionamento',
        dentro do limite de dias.

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com colunas compatíveis com o gerador
            de aprovisionamento.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_consulta_siebel_corrente'):
                logger.warning(
                    "View vw_consulta_siebel_corrente não encontrada "
                    "no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'cs')

            query = f"""
                SELECT
                    COALESCE(
                        NULLIF(TRIM(cs.proposta_isize), ''),
                        NULLIF(TRIM(cs.codigo_externo), ''),
                        ''
                    ) AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        NULLIF(TRIM(cs.cpf), ''),
                        NULLIF(TRIM(l.documento), ''),
                        ''
                    ) AS cpf,
                    cs.numero_acesso,
                    cs.numero_ordem,
                    cs.codigo_externo       AS proposta_isize_original,
                    cs.numero_temporario,
                    cs.bilhete_temporario,
                    cs.numero_bilhete,
                    cs.status_bilhete,
                    cs.operadora_doadora,
                    cs.data_portabilidade,
                    cs.motivo_recusa,
                    cs.motivo_cancelamento,
                    cs.ultimo_bilhete,
                    cs.status_ordem,
                    cs.preco_ordem,
                    cs.data_conclusao_ordem,
                    cs.motivo_nao_consultado,
                    cs.motivo_nao_cancelado,
                    cs.motivo_nao_aberto,
                    cs.motivo_nao_reagendado,
                    cs.novo_status_bilhete,
                    cs.nova_data_portabilidade,
                    cs.responsavel_processamento,
                    cs.data_inicial_processamento,
                    cs.data_final_processamento,
                    cs.registro_valido,
                    cs.ajustes_registro,
                    cs.numero_acesso_valido,
                    cs.ajustes_numero_acesso,
                    l.status                AS status_entrega,
                    l.data_entrega,
                    l.iccid,
                    l.nu_pedido,
                    l.ultima_ocorrencia,
                    l.data_ultima_ocorrencia,
                    p.data_venda,
                    p.produto,
                    p.plano
                FROM vw_consulta_siebel_corrente cs
                LEFT JOIN vw_base_unificada bu
                    ON bu.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_logistica_corrente l
                    ON l.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = cs.proposta_isize
                WHERE cs.status_ordem = 'Em Aprovisionamento'
                  AND (
                      p.data_venda IS NULL
                      OR p.data_venda >= :data_limite
                  )
                  {rejeicao_filter}
                ORDER BY cs.created_at DESC
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_aprovisionamento: %d registros "
                    "encontrados (últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_aprovisionamento: %s", e
                )
                return []

    # ------------------------------------------------------------------
    # 3. Registros de reabertura (cancelados)
    # ------------------------------------------------------------------

    def buscar_registros_reabertura(
        self, dias_limite: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Busca registros de reabertura (portabilidade cancelada).

        Retorna registros com status_bilhete = 'Portabilidade Cancelada'
        ou motivo_cancelamento preenchido, dentro do limite de dias.

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com colunas compatíveis com o gerador
            de reabertura.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_consulta_siebel_corrente'):
                logger.warning(
                    "View vw_consulta_siebel_corrente não encontrada "
                    "no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'cs')

            query = f"""
                SELECT
                    COALESCE(
                        NULLIF(TRIM(cs.proposta_isize), ''),
                        NULLIF(TRIM(cs.codigo_externo), ''),
                        ''
                    ) AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        NULLIF(TRIM(cs.cpf), ''),
                        NULLIF(TRIM(l.documento), ''),
                        ''
                    ) AS cpf,
                    cs.numero_acesso,
                    cs.numero_ordem,
                    cs.numero_temporario,
                    cs.bilhete_temporario,
                    cs.numero_bilhete,
                    cs.status_bilhete,
                    cs.operadora_doadora,
                    cs.data_portabilidade,
                    cs.motivo_recusa,
                    cs.motivo_cancelamento,
                    cs.ultimo_bilhete,
                    cs.status_ordem,
                    cs.preco_ordem,
                    cs.data_conclusao_ordem,
                    cs.motivo_nao_consultado,
                    cs.motivo_nao_cancelado,
                    cs.motivo_nao_aberto,
                    cs.motivo_nao_reagendado,
                    cs.novo_status_bilhete,
                    cs.nova_data_portabilidade,
                    cs.responsavel_processamento,
                    cs.data_inicial_processamento,
                    cs.data_final_processamento,
                    cs.registro_valido,
                    cs.ajustes_registro,
                    cs.numero_acesso_valido,
                    cs.ajustes_numero_acesso,
                    c.nome_cliente,
                    c.endereco,
                    c.numero,
                    c.complemento,
                    c.bairro,
                    c.cidade,
                    c.uf,
                    c.cep,
                    p.data_venda,
                    p.produto,
                    p.plano
                FROM vw_consulta_siebel_corrente cs
                LEFT JOIN vw_base_unificada bu
                    ON bu.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_logistica_corrente l
                    ON l.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_clientes_corrente c
                    ON c.cpf = cs.cpf
                WHERE (
                    cs.status_bilhete = 'Portabilidade Cancelada'
                    OR (
                        cs.motivo_cancelamento IS NOT NULL
                        AND cs.motivo_cancelamento != ''
                        AND cs.motivo_cancelamento != 'NULL'
                    )
                )
                AND (
                    p.data_venda IS NULL
                    OR p.data_venda >= :data_limite
                )
                {rejeicao_filter}
                ORDER BY cs.created_at DESC
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_reabertura: %d registros "
                    "encontrados (últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_reabertura: %s", e
                )
                return []

    # ------------------------------------------------------------------
    # 4. Registros para consulta (entregues)
    # ------------------------------------------------------------------

    def buscar_registros_consulta(
        self, dias_limite: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Busca registros para consulta (vendas com confirmação de entrega).

        Retorna apenas as colunas necessárias para o gerador de consulta:
        codigo_externo, cpf, numero_acesso, numero_ordem,
        telefone_portado, numero_linha.

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com colunas compatíveis com o gerador
            de consulta.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_base_unificada'):
                logger.warning(
                    "View vw_base_unificada não encontrada no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'bu')

            query = f"""
                SELECT DISTINCT
                    bu.proposta_isize       AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        NULLIF(TRIM(l.documento), ''),
                        ''
                    ) AS cpf,
                    COALESCE(bu.numero_acesso, '') AS numero_acesso,
                    COALESCE(bu.numero_ordem, '') AS numero_ordem,
                    bu.telefone_portabilidade AS telefone_portado,
                    bu.numero_linha
                FROM vw_base_unificada bu
                LEFT JOIN vw_logistica_corrente l
                    ON l.proposta_isize = bu.proposta_isize
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = bu.proposta_isize
                WHERE (
                    LOWER(COALESCE(bu.status_logistica, ''))
                        LIKE '%entregue%'
                    OR LOWER(COALESCE(bu.status_logistica, ''))
                        LIKE '%pedido entregue%'
                    OR LOWER(COALESCE(l.ultima_ocorrencia, ''))
                        LIKE '%entregue%'
                    OR LOWER(COALESCE(l.ultima_ocorrencia, ''))
                        LIKE '%pedido entregue%'
                )
                AND (
                    p.data_venda IS NULL
                    OR p.data_venda >= :data_limite
                )
                {rejeicao_filter}
                ORDER BY p.data_venda DESC NULLS LAST
                LIMIT 20000
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_consulta: %d registros com "
                    "entrega confirmada (últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_consulta: %s", e
                )
                return []

    # ------------------------------------------------------------------
    # 5. Registros com erro no aprovisionamento
    # ------------------------------------------------------------------

    def buscar_registros_erro_aprovisionamento(
        self, dias_limite: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Busca registros com erro no aprovisionamento.

        Retorna registros com status_ordem = 'Erro no Aprovisionamento'
        ou status_bilhete = 'Erro no Aprovisionamento', dentro do
        limite de dias.

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com colunas compatíveis com o gerador
            de erro de aprovisionamento.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_consulta_siebel_corrente'):
                logger.warning(
                    "View vw_consulta_siebel_corrente não encontrada "
                    "no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'cs')

            query = f"""
                SELECT
                    COALESCE(
                        NULLIF(TRIM(cs.proposta_isize), ''),
                        NULLIF(TRIM(cs.codigo_externo), ''),
                        ''
                    ) AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        NULLIF(TRIM(cs.cpf), ''),
                        NULLIF(TRIM(l.documento), ''),
                        ''
                    ) AS cpf,
                    cs.numero_acesso,
                    cs.numero_ordem,
                    cs.numero_temporario,
                    cs.bilhete_temporario,
                    cs.numero_bilhete,
                    cs.status_bilhete,
                    cs.operadora_doadora,
                    cs.data_portabilidade,
                    cs.motivo_recusa,
                    cs.motivo_cancelamento,
                    cs.ultimo_bilhete,
                    cs.status_ordem,
                    cs.preco_ordem,
                    cs.data_conclusao_ordem,
                    cs.motivo_nao_consultado,
                    cs.motivo_nao_cancelado,
                    cs.motivo_nao_aberto,
                    cs.motivo_nao_reagendado,
                    cs.novo_status_bilhete,
                    cs.nova_data_portabilidade,
                    cs.responsavel_processamento,
                    cs.data_inicial_processamento,
                    cs.data_final_processamento,
                    cs.registro_valido,
                    cs.ajustes_registro,
                    cs.numero_acesso_valido,
                    cs.ajustes_numero_acesso,
                    l.status                AS status_entrega,
                    l.data_entrega,
                    l.iccid,
                    l.nu_pedido,
                    l.ultima_ocorrencia,
                    l.data_ultima_ocorrencia,
                    p.data_venda,
                    p.produto,
                    p.plano,
                    COUNT(*) OVER (
                        PARTITION BY cs.proposta_isize
                    ) AS total_classificacoes
                FROM vw_consulta_siebel_corrente cs
                LEFT JOIN vw_base_unificada bu
                    ON bu.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_logistica_corrente l
                    ON l.proposta_isize = cs.proposta_isize
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = cs.proposta_isize
                WHERE (
                    cs.status_ordem = 'Erro no Aprovisionamento'
                    OR cs.status_bilhete = 'Erro no Aprovisionamento'
                )
                AND (
                    p.data_venda IS NULL
                    OR p.data_venda >= :data_limite
                )
                {rejeicao_filter}
                ORDER BY cs.created_at DESC
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_erro_aprovisionamento: %d "
                    "registros encontrados (últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_erro_aprovisionamento: "
                    "%s", e,
                )
                return []

    # ------------------------------------------------------------------
    # 6. Registros de entrega/baixa (status problemático)
    # ------------------------------------------------------------------

    def buscar_registros_entrega_baixa(
        self, dias_limite: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Busca registros com status de entrega problemática.

        Usa vw_base_unificada + vw_logistica_corrente +
        vw_clientes_corrente + vw_propostas_corrente.
        vw_logistica_corrente já retorna a versão mais recente por
        proposta_isize (a view trata MAX(versao)).

        Filtros:
        - Últimos ``dias_limite`` dias por data de venda
        - Status de entrega contém: cancelad, baixa, remetente,
          aguardando correios, extravi
        - Exclusão de Rejeição SMS
        - Fallback de CPF via COALESCE

        Args:
            dias_limite: Número de dias para trás a considerar.

        Returns:
            Lista de dicts com aliases compatíveis com
            gerar_homologacao_entrega_baixa.py.
        """
        data_limite = (
            datetime.now() - timedelta(days=dias_limite)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            if not self._view_exists(conn, 'vw_base_unificada'):
                logger.warning(
                    "View vw_base_unificada não encontrada no banco v2"
                )
                return []

            rejeicao_filter = self._rejeicao_sms_filter(conn, 'bu')

            query = f"""
                SELECT DISTINCT
                    COALESCE(
                        NULLIF(TRIM(bu.proposta_isize), ''),
                        ''
                    ) AS codigo_externo,
                    COALESCE(
                        NULLIF(TRIM(bu.cpf), ''),
                        NULLIF(TRIM(l.documento), ''),
                        ''
                    ) AS cpf,
                    bu.nome_cliente         AS cliente_nome,
                    bu.telefone_portabilidade AS telefone_portado,
                    p.data_venda,
                    sv.data_conectada,
                    p.plano,
                    c.endereco,
                    c.numero,
                    c.complemento,
                    c.bairro,
                    c.cidade,
                    c.uf,
                    c.cep,
                    c.ponto_referencia,
                    bu.status_venda         AS crivo_vendas,
                    COALESCE(
                        l.status,
                        l.ultima_ocorrencia,
                        ''
                    ) AS status_entrega_coverte,
                    l.status                AS ro_status,
                    l.ultima_ocorrencia     AS ro_ultima_ocorrencia,
                    l.rastreio,
                    l.nu_pedido
                FROM vw_base_unificada bu
                LEFT JOIN vw_logistica_corrente l
                    ON l.proposta_isize = bu.proposta_isize
                LEFT JOIN vw_clientes_corrente c
                    ON c.cpf = bu.cpf
                LEFT JOIN vw_propostas_corrente p
                    ON p.proposta_isize = bu.proposta_isize
                LEFT JOIN status_venda sv
                    ON sv.proposta_isize = bu.proposta_isize
                    AND sv.versao = (
                        SELECT MAX(sv2.versao)
                        FROM status_venda sv2
                        WHERE sv2.proposta_isize = bu.proposta_isize
                    )
                WHERE bu.proposta_isize IS NOT NULL
                  AND TRIM(COALESCE(bu.proposta_isize, '')) != ''
                  AND (
                      (l.status IS NOT NULL AND l.status != ''
                       AND (
                           LOWER(l.status) LIKE '%cancelad%'
                           OR LOWER(l.status) LIKE '%baixa%'
                           OR LOWER(l.status) LIKE '%remetente%'
                           OR LOWER(l.status) LIKE '%aguardando correios%'
                           OR LOWER(l.status) LIKE '%extravi%'
                       ))
                      OR (l.ultima_ocorrencia IS NOT NULL
                          AND l.ultima_ocorrencia != ''
                          AND (
                              LOWER(l.ultima_ocorrencia) LIKE '%cancelad%'
                              OR LOWER(l.ultima_ocorrencia) LIKE '%baixa%'
                              OR LOWER(l.ultima_ocorrencia)
                                  LIKE '%remetente%'
                              OR LOWER(l.ultima_ocorrencia)
                                  LIKE '%aguardando correios%'
                              OR LOWER(l.ultima_ocorrencia)
                                  LIKE '%extravi%'
                          ))
                  )
                  AND (
                      p.data_venda IS NULL
                      OR p.data_venda >= :data_limite
                  )
                  {rejeicao_filter}
                ORDER BY p.data_venda DESC NULLS LAST,
                         bu.proposta_isize DESC
                LIMIT 15000
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_entrega_baixa: %d registros "
                    "encontrados (últimos %d dias)",
                    len(rows), dias_limite,
                )
                return self._rows_to_dicts(rows)
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_entrega_baixa: %s", e
                )
                return []
