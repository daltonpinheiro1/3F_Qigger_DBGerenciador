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
        self, periodo_dias: int = 90
    ) -> List[Dict[str, Any]]:
        """
        Busca registros que necessitam tratamento/correção.

        Cabeçalho padronizado conforme layout de importação Siebel.
        Inclui: endereço problemático, reprovadas, quebra logística.
        Exclui: entregues.

        Returns:
            Lista de dicts no layout padronizado.
        """
        data_limite = (
            datetime.now() - timedelta(days=periodo_dias)
        ).strftime('%Y-%m-%d')

        with self._conn() as conn:
            try:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM cache_base_unificada"
                )
                if cur.fetchone()[0] == 0:
                    logger.error("cache_base_unificada vazia.")
                    return []
            except sqlite3.OperationalError:
                logger.error("Tabela cache_base_unificada não existe.")
                return []

            query = """
                SELECT
                    -- CPF: nunca 0, fallback para propostas
                    CASE
                        WHEN CAST(REPLACE(REPLACE(REPLACE(COALESCE(c.cpf, ''), '.', ''), '-', ''), '/', '') AS INTEGER) > 0
                        THEN PRINTF('%011d', CAST(REPLACE(REPLACE(REPLACE(c.cpf, '.', ''), '-', ''), '/', '') AS INTEGER))
                        WHEN CAST(REPLACE(REPLACE(REPLACE(COALESCE(cb.cpf, ''), '.', ''), '-', ''), '/', '') AS INTEGER) > 0
                        THEN PRINTF('%011d', CAST(REPLACE(REPLACE(REPLACE(cb.cpf, '.', ''), '-', ''), '/', '') AS INTEGER))
                        ELSE ''
                    END AS "Cpf",
                    COALESCE(NULLIF(TRIM(c.nome_cliente), ''), TRIM(cb.nome_cliente)) AS "Nome do cliente",
                    COALESCE(NULLIF(TRIM(c.data_nascimento), ''), '') AS "Data de nascimento",
                    COALESCE(NULLIF(TRIM(c.nome_mae), ''), '') AS "Nome da mãe",
                    REPLACE(TRIM(COALESCE(c.cep, '')), '-', '') AS "Cep",
                    UPPER(TRIM(COALESCE(c.uf, ''))) AS "Estado",
                    UPPER(TRIM(COALESCE(c.endereco, ''))) AS "Logradouro",
                    TRIM(COALESCE(c.numero, '')) AS "Número",
                    TRIM(COALESCE(c.complemento, '')) AS "Complemento",
                    TRIM(COALESCE(c.ponto_referencia, '')) AS "Referência",
                    UPPER(TRIM(COALESCE(c.bairro, ''))) AS "Bairro",
                    UPPER(TRIM(COALESCE(c.cidade, ''))) AS "Cidade",
                    LOWER(TRIM(COALESCE(c.email, ''))) AS "Email",
                    -- Telefone de contato 1: numero portado ou ddd+telefone padronizado 11 dígitos
                    CASE
                        WHEN COALESCE(TRIM(cb.telefone_portabilidade), '') != ''
                         AND TRIM(cb.telefone_portabilidade) != '-'
                         AND LENGTH(REPLACE(REPLACE(REPLACE(TRIM(cb.telefone_portabilidade), '-', ''), ' ', ''), '(', '')) >= 10
                        THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cb.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', '')
                        WHEN COALESCE(TRIM(c.ddd_1), '') != '' AND COALESCE(TRIM(c.telefone_1), '') != ''
                        THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.ddd_1), '-', ''), ' ', ''), '(', ''), ')', '')
                             || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.telefone_1), '-', ''), ' ', ''), '(', ''), ')', '')
                        ELSE ''
                    END AS "Telefone de contato 1",
                    '' AS "Telefone de contato 2",
                    '' AS "Telefone de contato 3",
                    TRIM(COALESCE(cb.plano, '')) AS "Plano",
                    '' AS "Preço",
                    '' AS "Tipo do pagamento",
                    '' AS "Tipo da fatura",
                    '' AS "Banco",
                    '' AS "Agência",
                    '' AS "Conta",
                    '' AS "Data de vencimento da fatura",
                    -- Número da portabilidade
                    CASE
                        WHEN COALESCE(TRIM(cb.telefone_portabilidade), '') != ''
                         AND TRIM(cb.telefone_portabilidade) != '-'
                         AND LENGTH(REPLACE(REPLACE(REPLACE(TRIM(cb.telefone_portabilidade), '-', ''), ' ', ''), '(', '')) >= 10
                        THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cb.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', '')
                        ELSE ''
                    END AS "Número da portabilidade",
                    -- Tim chip / ICCID: último valor, se múltiplos separados por vírgula pegar o último
                    (SELECT
                        CASE
                            WHEN INSTR(l.iccid, ',') > 0
                            THEN TRIM(SUBSTR(l.iccid, LENGTH(l.iccid) - INSTR(REPLACE(REPLACE(l.iccid, ' ', ''), ',', CHAR(0) || ','), CHAR(0)) + 2))
                            ELSE TRIM(l.iccid)
                        END
                     FROM logistica l
                     WHERE l.proposta_isize = cb.proposta_isize
                       AND COALESCE(TRIM(l.iccid), '') != ''
                     ORDER BY l.id DESC LIMIT 1
                    ) AS "Tim chip",
                    STRFTIME('%d/%m/%Y', DATE(cb.data_venda)) AS "Data da venda",
                    '' AS "Tem número provisório?",
                    '' AS "Tem portabilidade antecipada?",
                    '' AS "Canal",
                    cb.proposta_isize AS "Código externo",
                    TRIM(COALESCE(cb.rastreio, '')) AS "Rastreio",
                    -- Motivo do reprocessamento
                    CASE
                        WHEN c.cep IS NULL
                          OR TRIM(COALESCE(c.cep, '')) = ''
                          OR LENGTH(REPLACE(TRIM(COALESCE(c.cep, '')), '-', '')) < 8
                        THEN 'CEP_INVALIDO'
                        WHEN c.endereco IS NULL OR TRIM(COALESCE(c.endereco, '')) = ''
                        THEN 'ENDERECO_VAZIO'
                        WHEN c.cidade IS NULL OR TRIM(COALESCE(c.cidade, '')) = ''
                          OR c.uf IS NULL OR TRIM(COALESCE(c.uf, '')) = ''
                        THEN 'CIDADE_UF_VAZIO'
                        WHEN UPPER(TRIM(COALESCE(cb.status_logistica, '')))
                             IN ('ENTREGA CANCELADA', 'EM DEVOLUCAO AO REMETENTE',
                                 'DISTRIBUIDO AO REMETENTE', 'EXTRAVIADO',
                                 'INSERIDO NO BANCO DE DADOS')
                        THEN 'QUEBRA_LOGISTICA'
                        WHEN UPPER(TRIM(COALESCE(cb.status_venda, '')))
                             IN ('CANCELADA', 'CANCELADO', 'REJEITADA', 'REJEITADO')
                        THEN 'REPROVADA_CRIVO'
                        ELSE 'OUTRO'
                    END AS "motivo_reprocessamento",
                    UPPER(TRIM(COALESCE(cb.status_venda, ''))) AS "status_venda"
                FROM cache_base_unificada cb
                LEFT JOIN vw_clientes_corrente c ON c.cpf = cb.cpf
                WHERE cb.data_venda >= :data_limite
                  AND cb.proposta_isize IS NOT NULL
                  AND TRIM(COALESCE(cb.proposta_isize, '')) != ''
                  AND UPPER(TRIM(COALESCE(cb.status_logistica, '')))
                      NOT IN ('ENTREGUE', 'ENTREGUE COM ATRASO')
                  AND UPPER(TRIM(COALESCE(cb.status_logistica, '')))
                      NOT LIKE '%ENTREGUE%'
                  AND (
                      c.cep IS NULL
                      OR TRIM(COALESCE(c.cep, '')) = ''
                      OR LENGTH(REPLACE(TRIM(COALESCE(c.cep, '')), '-', '')) < 8
                      OR c.endereco IS NULL OR TRIM(COALESCE(c.endereco, '')) = ''
                      OR c.cidade IS NULL OR TRIM(COALESCE(c.cidade, '')) = ''
                      OR c.uf IS NULL OR TRIM(COALESCE(c.uf, '')) = ''
                      OR UPPER(TRIM(COALESCE(cb.status_venda, '')))
                         IN ('CANCELADA', 'CANCELADO', 'REJEITADA', 'REJEITADO')
                      OR UPPER(TRIM(COALESCE(cb.status_logistica, '')))
                         IN ('ENTREGA CANCELADA', 'EM DEVOLUCAO AO REMETENTE',
                             'DISTRIBUIDO AO REMETENTE', 'EXTRAVIADO',
                             'INSERIDO NO BANCO DE DADOS')
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
                    "para tratamento (últimos %d dias)",
                    len(rows), periodo_dias,
                )
                return [dict(r) for r in rows]
            except sqlite3.OperationalError as e:
                logger.error(
                    "Erro em buscar_registros_reprocessamento: %s", e
                )
                return []
