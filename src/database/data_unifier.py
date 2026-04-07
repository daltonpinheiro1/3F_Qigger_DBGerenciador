"""
Unificador de dados para o banco de dados v2 de portabilidade.

Gerencia o cache materializado (cache_base_unificada) e fornece
métodos de consulta unificada por proposta, CPF e telefone.
"""

import logging
from typing import Any, Dict, List, Optional

from src.database.db_manager_v2 import DatabaseManagerV2

logger = logging.getLogger(__name__)


class DataUnifier:
    """
    Unificador de dados que opera sobre o cache materializado
    (tabela ``cache_base_unificada``) do banco v2.

    Permite atualizar o cache a partir da view ``vw_base_unificada``
    e realizar buscas rápidas por proposta, CPF ou telefone.
    """

    def __init__(self, db_v2: DatabaseManagerV2):
        """
        Inicializa o unificador.

        Args:
            db_v2: Instância de DatabaseManagerV2 já inicializada.
        """
        self.db_v2 = db_v2
        logger.info("DataUnifier inicializado")

    # ------------------------------------------------------------------
    # Atualização de cache
    # ------------------------------------------------------------------

    def atualizar_cache(self, proposta_isize: str) -> None:
        """
        Atualiza o cache materializado para uma proposta específica.

        Delega para ``DatabaseManagerV2.atualizar_cache_unificada``.

        Args:
            proposta_isize: Identificador da proposta.
        """
        self.db_v2.atualizar_cache_unificada(proposta_isize)

    def atualizar_cache_lote(self, propostas_isize: List[str]) -> Dict[str, int]:
        """
        Atualiza o cache materializado para múltiplas propostas.

        Args:
            propostas_isize: Lista de identificadores de propostas.

        Returns:
            Dicionário com contagens: atualizados e erros.
        """
        atualizados = 0
        erros = 0
        total = len(propostas_isize)

        for idx, proposta in enumerate(propostas_isize, 1):
            try:
                self.db_v2.atualizar_cache_unificada(proposta)
                atualizados += 1
            except Exception as e:
                erros += 1
                logger.warning("Erro ao atualizar cache para %s: %s", proposta, e)

            if idx % 500 == 0:
                logger.info("Cache lote: %d/%d atualizados", idx, total)

        logger.info(
            "Cache lote concluído: atualizados=%d, erros=%d, total=%d",
            atualizados, erros, total,
        )
        return {'atualizados': atualizados, 'erros': erros}

    # ------------------------------------------------------------------
    # Consultas
    # ------------------------------------------------------------------

    def buscar_unificado(self, proposta_isize: str) -> Optional[Dict[str, Any]]:
        """
        Busca dados unificados de uma proposta.

        Consulta primeiro o cache (``cache_base_unificada``). Se não
        encontrado, consulta a view ``vw_base_unificada`` como fallback.

        Args:
            proposta_isize: Identificador da proposta.

        Returns:
            Dicionário com os dados unificados ou None.
        """
        with self.db_v2._get_connection() as conn:
            cursor = conn.cursor()

            # Tentar cache primeiro
            cursor.execute(
                "SELECT * FROM cache_base_unificada WHERE proposta_isize = ?",
                (proposta_isize,),
            )
            row = cursor.fetchone()
            if row:
                return dict(row)

            # Fallback: view
            try:
                cursor.execute(
                    "SELECT * FROM vw_base_unificada WHERE proposta_isize = ?",
                    (proposta_isize,),
                )
                row = cursor.fetchone()
                if row:
                    return dict(row)
            except Exception as e:
                logger.debug("Fallback vw_base_unificada falhou: %s", e)

        return None

    def buscar_por_cpf(self, cpf: str) -> List[Dict[str, Any]]:
        """
        Busca registros unificados por CPF no cache.

        Args:
            cpf: CPF do cliente (apenas dígitos).

        Returns:
            Lista de dicionários com os dados encontrados.
        """
        with self.db_v2._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM cache_base_unificada WHERE cpf = ?",
                (cpf,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def buscar_por_telefone(self, telefone: str) -> List[Dict[str, Any]]:
        """
        Busca registros unificados por telefone de portabilidade no cache.

        Args:
            telefone: Número de telefone de portabilidade.

        Returns:
            Lista de dicionários com os dados encontrados.
        """
        with self.db_v2._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM cache_base_unificada WHERE telefone_portabilidade = ?",
                (telefone,),
            )
            return [dict(row) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    # Manutenção
    # ------------------------------------------------------------------

    def reconstruir_cache_completo(self) -> Dict[str, int]:
        """
        Reconstrói o cache materializado inteiro a partir de ``vw_base_unificada``.

        Usa INSERT INTO ... SELECT FROM para reconstruir em uma única operação SQL,
        muito mais rápido que atualizar proposta por proposta.

        Returns:
            Dicionário com contagens: inseridos e erros.
        """
        inseridos = 0
        erros = 0

        with self.db_v2._get_connection() as conn:
            cursor = conn.cursor()

            # Limpar cache existente
            cursor.execute("DELETE FROM cache_base_unificada")
            conn.commit()
            logger.info("Cache limpo para reconstrução")

            # Reconstruir em uma única operação SQL (INSERT INTO ... SELECT FROM)
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO cache_base_unificada (
                        proposta_isize, cpf, nome_cliente, telefone_portabilidade,
                        numero_linha, numero_ordem, data_venda, produto, plano,
                        status_venda, portabilidade_status, status_tim,
                        data_ativacao_tim, status_logistica, rastreio, data_entrega,
                        data_gross, classificacao_cr, resultado_gross,
                        status_pedido, detalhe_status, status_bilhete, status_ordem,
                        bluechip_status, pedido_bluechip, regra_id,
                        acao_a_realizar, tipo_mensagem, atualizado_em
                    )
                    SELECT
                        proposta_isize, cpf, nome_cliente, telefone_portabilidade,
                        numero_linha, numero_ordem, data_venda, produto, plano,
                        status_venda, portabilidade_status, status_tim,
                        data_ativacao_tim, status_logistica,
                        rastreio_logistica,
                        data_entrega,
                        data_gross, classificacao_cr, resultado_gross,
                        status_pedido, detalhe_status, status_bilhete, status_ordem,
                        bluechip_status, pedido_bluechip, regra_id,
                        acao_a_realizar, tipo_mensagem, CURRENT_TIMESTAMP
                    FROM vw_base_unificada
                    GROUP BY proposta_isize
                """)
                inseridos = cursor.rowcount
                conn.commit()
                logger.info(
                    "Reconstrução do cache concluída (bulk INSERT): %d inseridos",
                    inseridos,
                )
            except Exception as e:
                erros = 1
                logger.error("Erro ao reconstruir cache (bulk): %s", e)
                # Fallback: tentar proposta por proposta se bulk falhar
                logger.info("Tentando reconstrução proposta por proposta (fallback)...")
                try:
                    cursor.execute("SELECT proposta_isize FROM vw_base_unificada")
                    propostas = [row[0] for row in cursor.fetchall()]
                except Exception as e2:
                    logger.error("Erro ao consultar vw_base_unificada: %s", e2)
                    return {'inseridos': 0, 'erros': 1}

                inseridos = 0
                erros = 0
                for proposta in propostas:
                    try:
                        self.db_v2.atualizar_cache_unificada(proposta)
                        inseridos += 1
                    except Exception as e3:
                        erros += 1
                    if inseridos % 5000 == 0:
                        logger.info("Reconstrução fallback: %d/%d inseridos", inseridos, len(propostas))

        logger.info(
            "Reconstrução do cache concluída: inseridos=%d, erros=%d",
            inseridos, erros,
        )
        return {'inseridos': inseridos, 'erros': erros}
