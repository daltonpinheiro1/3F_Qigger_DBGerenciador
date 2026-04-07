"""Query TIM_REPROCESSAMENTO adaptada para views V2."""
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class QueriesReprocessamento:
    """Queries para o módulo de reprocessamento de endereços."""

    def __init__(self, db_v2_path: str):
        self.db_path = db_v2_path

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def buscar_registros_reprocessamento(
        self, periodo_dias: int = 180
    ) -> List[Dict[str, Any]]:
        """
        Busca registros que necessitam correção de endereço.

        Usa cache_base_unificada + vw_clientes_corrente para performance.
        Filtra apenas registros com endereço problemático (CEP vazio/inválido,
        endereço/cidade/UF vazios). vw_clientes_corrente já retorna a versão
        mais recente, então correções já importadas são automaticamente
        excluídas.

        Returns:
            Lista de dicts com dados de proposta + endereço + logística.
        """
        data_limite = (
            datetime.now() - timedelta(days=periodo_dias)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            # Verificar se cache existe e tem dados
            try:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM cache_base_unificada"
                )
                total_cache = cur.fetchone()[0]
                if total_cache == 0:
                    logger.error(
                        "cache_base_unificada vazia. "
                        "Execute ETAPA 3b primeiro."
                    )
                    return []
            except sqlite3.OperationalError:
                logger.error("Tabela cache_base_unificada não existe.")
                return []

            query = """
                SELECT
                    cb.proposta_isize,
                    cb.cpf,
                    cb.nome_cliente,
                    cb.telefone_portabilidade,
                    cb.data_venda,
                    cb.status_venda,
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
                    cb.produto,
                    cb.plano,
                    cb.status_logistica,
                    cb.rastreio
                FROM cache_base_unificada cb
                LEFT JOIN vw_clientes_corrente c
                    ON c.cpf = cb.cpf
                WHERE cb.data_venda >= :data_limite
                  AND cb.proposta_isize IS NOT NULL
                  AND TRIM(COALESCE(cb.proposta_isize, '')) != ''
                  AND (
                      c.cep IS NULL
                      OR TRIM(COALESCE(c.cep, '')) = ''
                      OR LENGTH(REPLACE(TRIM(COALESCE(c.cep, '')),
                                        '-', '')) < 8
                      OR c.endereco IS NULL
                      OR TRIM(COALESCE(c.endereco, '')) = ''
                      OR c.cidade IS NULL
                      OR TRIM(COALESCE(c.cidade, '')) = ''
                      OR c.uf IS NULL
                      OR TRIM(COALESCE(c.uf, '')) = ''
                  )
                ORDER BY cb.data_venda DESC
            """
            try:
                cursor = conn.execute(
                    query, {'data_limite': data_limite}
                )
                rows = cursor.fetchall()
                logger.info(
                    "buscar_registros_reprocessamento: %d registros "
                    "com endereço problemático (últimos %d dias)",
                    len(rows), periodo_dias,
                )
                return [dict(r) for r in rows]
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_reprocessamento: %s", e
                )
                return []
