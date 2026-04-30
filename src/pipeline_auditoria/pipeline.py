"""
Orquestrador do pipeline de auditoria de vendas TIM Pré/Controle.

Coordena as três etapas: coleta EVA, processamento de retornos RPA
e cruzamento dos dados para gerar a auditoria consolidada.
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import (
    EVA_DATABASE,
    EVA_PASSWORD,
    EVA_SERVER,
    EVA_USER,
    EVA_VIEW,
    PASTA_IMPORTACOES,
)
from src.database.conector_eva import ConectorEVA
from src.pipeline_auditoria.classificador_status import ClassificadorStatus
from src.pipeline_auditoria.processador_retorno_rpa import ProcessadorRetornoRPA

logger = logging.getLogger(__name__)


class PipelineAuditoria:
    """Orquestrador do pipeline de auditoria de vendas TIM Pré/Controle.

    Coordena três etapas:
    1. Coleta de vendas do EVA (SQL Server)
    2. Processamento de retornos RPA (CSV)
    3. Cruzamento EVA × RPA → auditoria_vendas_tim
    """

    def __init__(self, db_manager):
        """Inicializa o pipeline com instâncias dos componentes.

        Args:
            db_manager: Instância de DatabaseManagerV2 (duck typing).
        """
        self.db_manager = db_manager
        self.classificador = ClassificadorStatus()
        self.processador_rpa = ProcessadorRetornoRPA(self.classificador)

        # ConectorEVA — pode falhar se credenciais ausentes
        self._conector_eva = None
        try:
            self._conector_eva = ConectorEVA({
                "EVA_SERVER": EVA_SERVER,
                "EVA_DATABASE": EVA_DATABASE,
                "EVA_USER": EVA_USER,
                "EVA_PASSWORD": EVA_PASSWORD,
                "EVA_VIEW": EVA_VIEW,
            })
        except ValueError as e:
            logger.warning("ConectorEVA não disponível: %s", e)

    # ------------------------------------------------------------------
    # Etapa 1: Coleta EVA
    # ------------------------------------------------------------------

    def _etapa_coleta_eva(self) -> dict:
        """Coleta vendas do EVA via SQL Server.

        Returns:
            Estatísticas {lote_id, total, inseridos, erros}.
        """
        stats = {"lote_id": None, "total": 0, "inseridos": 0, "erros": 0}

        if self._conector_eva is None:
            logger.warning(
                "Etapa EVA ignorada: conector não disponível"
            )
            return stats

        try:
            if not self._conector_eva.conectar():
                logger.warning(
                    "Etapa EVA ignorada: falha na conexão"
                )
                return stats

            stats = self._conector_eva.coletar_vendas(self.db_manager)
        except Exception:
            logger.exception("Erro na etapa de coleta EVA")
        finally:
            if self._conector_eva is not None:
                self._conector_eva.desconectar()

        return stats

    # ------------------------------------------------------------------
    # Etapa 2: Retornos RPA
    # ------------------------------------------------------------------

    def _etapa_retornos_rpa(
        self, arquivos: List[str]
    ) -> List[Dict[str, Any]]:
        """Processa arquivos CSV de retorno RPA.

        Args:
            arquivos: Lista de caminhos para arquivos CSV.

        Returns:
            Lista de estatísticas por arquivo.
        """
        resultados = []
        for caminho in arquivos:
            logger.info("Processando retorno RPA: %s", caminho)
            try:
                stats = self.processador_rpa.processar_arquivo(
                    caminho, self.db_manager
                )
                resultados.append(stats)
            except Exception:
                logger.exception(
                    "Erro ao processar %s", caminho
                )
                resultados.append({
                    "lote_id": None,
                    "total": 0,
                    "inseridos": 0,
                    "erros": 0,
                })
        return resultados

    # ------------------------------------------------------------------
    # Etapa 3: Cruzamento
    # ------------------------------------------------------------------

    def _etapa_cruzamento(self) -> dict:
        """Cruza dados EVA × RPA e insere em auditoria_vendas_tim.

        Usa FULL OUTER JOIN approach:
        - LEFT JOIN RPA sobre EVA (registros com match + EVA sem RPA)
        - UNION registros RPA sem EVA

        Returns:
            Estatísticas {lote_id, total, inseridos, erros,
                          com_match, eva_sem_rpa, rpa_sem_eva,
                          distribuicao_status}.
        """
        stats = {
            "lote_id": None,
            "total": 0,
            "inseridos": 0,
            "erros": 0,
            "com_match": 0,
            "eva_sem_rpa": 0,
            "rpa_sem_eva": 0,
            "distribuicao_status": {},
        }

        with self.db_manager._get_connection() as conn:
            cursor = conn.cursor()

            # Consultar vendas EVA correntes (chave: numero_acesso)
            cursor.execute(
                "SELECT * FROM vw_vendas_eva_corrente"
            )
            vendas_eva = {
                row["numero_acesso"]: dict(row)
                for row in cursor.fetchall()
            }

            # Consultar retornos RPA correntes (chave: numero_acesso)
            cursor.execute(
                "SELECT * FROM vw_retornos_rpa_tim_corrente"
            )
            retornos_rpa = {
                row["numero_acesso"]: dict(row)
                for row in cursor.fetchall()
            }

        if not vendas_eva and not retornos_rpa:
            logger.info("Cruzamento: sem dados para cruzar")
            return stats

        # Gerar hash do cruzamento para o lote
        hash_dados = hashlib.sha256(
            json.dumps(
                {
                    "eva_keys": sorted(vendas_eva.keys()),
                    "rpa_keys": sorted(retornos_rpa.keys()),
                    "timestamp": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ).encode()
        ).hexdigest()

        lote_id = self.db_manager.criar_lote(
            nome_arquivo=(
                f"auditoria_vendas_{datetime.now():%Y%m%d_%H%M%S}"
            ),
            tipo="auditoria_vendas",
            hash_sha256=hash_dados,
        )
        stats["lote_id"] = lote_id

        # Chaves processadas (para identificar RPA sem EVA)
        rpa_processados = set()

        # 1) Iterar sobre EVA: match + EVA sem RPA (por numero_acesso)
        for numero_acesso, eva in vendas_eva.items():
            rpa = retornos_rpa.get(numero_acesso)

            if rpa:
                rpa_processados.add(numero_acesso)
                stats["com_match"] += 1
                registro = self._montar_registro_match(numero_acesso, eva, rpa)
            else:
                stats["eva_sem_rpa"] += 1
                registro = self._montar_registro_eva_sem_rpa(numero_acesso, eva)

            self._inserir_auditoria(
                registro, lote_id, stats
            )

        # 2) RPA sem EVA
        for numero_acesso, rpa in retornos_rpa.items():
            if numero_acesso not in rpa_processados:
                stats["rpa_sem_eva"] += 1
                registro = self._montar_registro_rpa_sem_eva(numero_acesso, rpa)
                self._inserir_auditoria(
                    registro, lote_id, stats
                )

        # Finalizar lote
        status_lote = (
            "concluido" if stats["erros"] == 0 else "erro"
        )
        self.db_manager.finalizar_lote(
            lote_id, stats["inseridos"], stats["erros"], status_lote
        )

        logger.info(
            "Cruzamento finalizado: %d com match, "
            "%d EVA sem RPA, %d RPA sem EVA",
            stats["com_match"],
            stats["eva_sem_rpa"],
            stats["rpa_sem_eva"],
        )

        return stats

    # ------------------------------------------------------------------
    # Helpers de montagem de registro
    # ------------------------------------------------------------------

    @staticmethod
    def _montar_registro_match(
        numero_acesso: str, eva: dict, rpa: dict
    ) -> dict:
        """Monta registro de auditoria para match EVA + RPA."""
        return {
            "numero_acesso": numero_acesso,
            "cod_venda": eva.get("cod_venda", ""),
            "operacao": eva.get("operacao", ""),
            "pedido": eva.get("pedido", ""),
            "id_atendimento": eva.get("id_atendimento", ""),
            "data_hora_gravacao": eva.get("data_hora_gravacao", ""),
            "data_emissao_eva": eva.get("data_emissao", ""),
            "nome_cliente": eva.get("nome_cliente", ""),
            "cpf": eva.get("cpf", ""),
            "telefone": eva.get("telefone", ""),
            "produto": eva.get("produto", ""),
            "plano": eva.get("plano", ""),
            "status_venda_eva": eva.get("status_venda", ""),
            "canal": eva.get("canal", ""),
            "equipe": eva.get("equipe", ""),
            "vendedor": eva.get("vendedor", ""),
            "supervisor": eva.get("supervisor", ""),
            "codigo_externo": rpa.get("codigo_externo", ""),
            "protocolo": rpa.get("protocolo", ""),
            "motivo_nao_migrado": rpa.get("motivo_nao_migrado", ""),
            "data_inicial_processamento": rpa.get(
                "data_inicial_processamento", ""
            ),
            "data_final_processamento": rpa.get(
                "data_final_processamento", ""
            ),
            "data_aprovacao": rpa.get("data_aprovacao", ""),
            "status_classificado": rpa.get(
                "status_classificado", "NAO PARAMETRIZADO"
            ),
            "vendas_eva_id": eva.get("id"),
            "retornos_rpa_tim_id": rpa.get("id"),
        }

    @staticmethod
    def _montar_registro_eva_sem_rpa(numero_acesso: str, eva: dict) -> dict:
        """Monta registro de auditoria para EVA sem retorno RPA."""
        return {
            "numero_acesso": numero_acesso,
            "cod_venda": eva.get("cod_venda", ""),
            "operacao": eva.get("operacao", ""),
            "pedido": eva.get("pedido", ""),
            "id_atendimento": eva.get("id_atendimento", ""),
            "data_hora_gravacao": eva.get("data_hora_gravacao", ""),
            "data_emissao_eva": eva.get("data_emissao", ""),
            "nome_cliente": eva.get("nome_cliente", ""),
            "cpf": eva.get("cpf", ""),
            "telefone": eva.get("telefone", ""),
            "produto": eva.get("produto", ""),
            "plano": eva.get("plano", ""),
            "status_venda_eva": eva.get("status_venda", ""),
            "canal": eva.get("canal", ""),
            "equipe": eva.get("equipe", ""),
            "vendedor": eva.get("vendedor", ""),
            "supervisor": eva.get("supervisor", ""),
            "codigo_externo": "",
            "protocolo": "",
            "motivo_nao_migrado": "",
            "data_inicial_processamento": "",
            "data_final_processamento": "",
            "data_aprovacao": "",
            "status_classificado": "PENDENTE_RETORNO",
            "vendas_eva_id": eva.get("id"),
            "retornos_rpa_tim_id": None,
        }

    @staticmethod
    def _montar_registro_rpa_sem_eva(numero_acesso: str, rpa: dict) -> dict:
        """Monta registro de auditoria para RPA sem correspondência EVA."""
        return {
            "numero_acesso": numero_acesso,
            "cod_venda": "",
            "operacao": "",
            "pedido": "",
            "id_atendimento": "",
            "data_hora_gravacao": "",
            "data_emissao_eva": "",
            "nome_cliente": "",
            "cpf": "",
            "telefone": "",
            "produto": "",
            "plano": "",
            "status_venda_eva": "",
            "canal": "",
            "equipe": "",
            "vendedor": "",
            "supervisor": "",
            "codigo_externo": rpa.get("codigo_externo", ""),
            "protocolo": rpa.get("protocolo", ""),
            "motivo_nao_migrado": rpa.get("motivo_nao_migrado", ""),
            "data_inicial_processamento": rpa.get(
                "data_inicial_processamento", ""
            ),
            "data_final_processamento": rpa.get(
                "data_final_processamento", ""
            ),
            "data_aprovacao": rpa.get("data_aprovacao", ""),
            "status_classificado": rpa.get(
                "status_classificado", "NAO PARAMETRIZADO"
            ),
            "vendas_eva_id": None,
            "retornos_rpa_tim_id": rpa.get("id"),
        }

    def _inserir_auditoria(
        self, registro: dict, lote_id: int, stats: dict
    ):
        """Insere um registro na tabela auditoria_vendas_tim."""
        stats["total"] += 1
        status = registro.get("status_classificado", "")
        stats["distribuicao_status"][status] = (
            stats["distribuicao_status"].get(status, 0) + 1
        )
        try:
            self.db_manager.inserir_registro(
                "auditoria_vendas_tim", registro, lote_id
            )
            stats["inseridos"] += 1
        except Exception:
            logger.exception(
                "Erro ao inserir auditoria: %s",
                registro.get("cod_venda", "?"),
            )
            stats["erros"] += 1

    # ------------------------------------------------------------------
    # Reportar estatísticas
    # ------------------------------------------------------------------

    @staticmethod
    def _reportar_estatisticas(stats: dict):
        """Loga estatísticas consolidadas do pipeline.

        Args:
            stats: Dicionário com estatísticas do pipeline.
        """
        logger.info("=" * 60)
        logger.info("ESTATÍSTICAS DO PIPELINE DE AUDITORIA")
        logger.info("=" * 60)

        eva = stats.get("eva", {})
        logger.info(
            "EVA: %d coletados, %d inseridos, %d erros",
            eva.get("total", 0),
            eva.get("inseridos", 0),
            eva.get("erros", 0),
        )

        rpa_list = stats.get("rpa", [])
        total_rpa = sum(r.get("inseridos", 0) for r in rpa_list)
        erros_rpa = sum(r.get("erros", 0) for r in rpa_list)
        logger.info(
            "RPA: %d arquivos, %d inseridos, %d erros",
            len(rpa_list), total_rpa, erros_rpa,
        )

        cruzamento = stats.get("cruzamento", {})
        logger.info(
            "Cruzamento: %d total, %d com match, "
            "%d EVA sem RPA, %d RPA sem EVA",
            cruzamento.get("total", 0),
            cruzamento.get("com_match", 0),
            cruzamento.get("eva_sem_rpa", 0),
            cruzamento.get("rpa_sem_eva", 0),
        )

        dist = cruzamento.get("distribuicao_status", {})
        if dist:
            logger.info("Distribuição de status:")
            for status, qtd in sorted(
                dist.items(), key=lambda x: -x[1]
            ):
                logger.info("  %s: %d", status, qtd)

        logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------------

    def executar(
        self, arquivos_rpa: Optional[List[str]] = None
    ) -> dict:
        """Executa o pipeline completo: EVA → RPA → Cruzamento.

        Args:
            arquivos_rpa: Lista de caminhos CSV. Se None, auto-detecta
                          em PASTA_RETORNOS_BACKOFFICE.

        Returns:
            Estatísticas consolidadas do pipeline.
        """
        logger.info("Iniciando pipeline de auditoria de vendas")
        inicio = datetime.now()

        # Etapa 1: Coleta EVA
        logger.info("--- Etapa 1: Coleta EVA ---")
        stats_eva = self._etapa_coleta_eva()

        # Etapa 2: Retornos RPA
        logger.info("--- Etapa 2: Retornos RPA ---")
        if arquivos_rpa is None:
            pasta = Path(PASTA_IMPORTACOES)
            if pasta.exists():
                arquivos_rpa = sorted(
                    str(f) for f in pasta.glob("*TIM*PRE*CRTL*Detalhado*.csv")
                    if f.is_file()
                )
                # Fallback: buscar por padrão mais amplo se nenhum encontrado
                if not arquivos_rpa:
                    arquivos_rpa = sorted(
                        str(f) for f in pasta.glob("*TIM*PRE*Detalhado*.csv")
                        if f.is_file()
                    )
                logger.info(
                    "Auto-detectados %d arquivos retorno RPA em %s",
                    len(arquivos_rpa), pasta,
                )
            else:
                arquivos_rpa = []
                logger.warning(
                    "Pasta de retornos não encontrada: %s", pasta
                )

        stats_rpa = self._etapa_retornos_rpa(arquivos_rpa)

        # Etapa 3: Cruzamento
        logger.info("--- Etapa 3: Cruzamento ---")
        stats_cruzamento = self._etapa_cruzamento()

        duracao = (datetime.now() - inicio).total_seconds()

        stats = {
            "eva": stats_eva,
            "rpa": stats_rpa,
            "cruzamento": stats_cruzamento,
            "duracao_segundos": duracao,
        }

        self._reportar_estatisticas(stats)

        logger.info(
            "Pipeline concluído em %.1f segundos", duracao
        )

        return stats
