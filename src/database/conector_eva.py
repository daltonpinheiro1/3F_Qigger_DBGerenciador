"""
Conector para o banco de dados EVA (SQL Server).

Responsável pela conexão com o SQL Server EVA e coleta de dados
de vendas da view vwSales, inserindo no banco V2 local.
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict

from config import BATCH_SIZE

logger = logging.getLogger(__name__)

# Credenciais obrigatórias para conexão EVA
REQUIRED_KEYS = ["EVA_SERVER", "EVA_USER", "EVA_PASSWORD"]

# Query completa da view vwSales
EVA_QUERY = """
SELECT OPERACAO, PEDIDO, ID_ATENDIMENTO, DATA_HORA_GRAVACAO,
       DATA_EMISSAO, ID_CAMPANHA, CAMPANHA, ID_FILA, FILA,
       SUPERVISOR, RESPONSAVEL_VENDEDOR, CLASSIFICACAO_VENDEDOR,
       COD_VENDA, CLIENTE, EMAIL, TELEFONE_DISCADOR, NOME_MAE,
       CPF, RG, RG_DATA_EMISSAO, RG_ORGAO_EXPEDITOR,
       [DATA NASCIMENTO], SEXO, CEP, UF, CIDADE, BAIRRO,
       [TIPO LOGRADOURO], ENDEREÇO, NUMERO, COMPLEMENTO,
       REFERENCIA, CATEGORIA, PRODUTO, VALOR, DESCONTO,
       VALOR_FINAL, GBBONUS, GBCORE, GBTOTAL, POSSUI_EMAIL,
       PLANO_URA, PLANO_URA_VALOR, ESIM, TELEFONE_VENDA,
       BOLETO_VIA_EMAIL, FORMA_PAGAMENTO, DIA_VENCIMENTO,
       BANCO_CODIGO, BANCO, CONTA_TIPO, AGENCIA, CONTA,
       CONTA_DIGITO, OBSERVACAO_VENDEDOR, NOME_BACKOFFICE,
       GRUPO_DE_STATUS_VENDA, STATUS_DO_VENDA, OBS_INTERNA_VENDA
FROM eva_activities.dbo.vwSales;
"""

# Campos que vão em colunas dedicadas na tabela vendas_eva
CAMPOS_DEDICADOS = {
    "OPERACAO": "operacao",
    "PEDIDO": "pedido",
    "ID_ATENDIMENTO": "id_atendimento",
    "DATA_HORA_GRAVACAO": "data_hora_gravacao",
    "DATA_EMISSAO": "data_emissao",
    "COD_VENDA": "cod_venda",
    "CLIENTE": "nome_cliente",
    "CPF": "cpf",
    "TELEFONE_DISCADOR": "telefone",
    "TELEFONE_VENDA": "numero_acesso",
    "PRODUTO": "produto",
    "PLANO_URA": "plano",
    "STATUS_DO_VENDA": "status_venda",
    "FILA": "canal",
    "CAMPANHA": "equipe",
    "RESPONSAVEL_VENDEDOR": "vendedor",
    "SUPERVISOR": "supervisor",
}


class ConectorEVA:
    """Conector para o banco de dados EVA (SQL Server).

    Estabelece conexão via pymssql (primário) ou pyodbc (fallback)
    e coleta dados de vendas da view vwSales.
    """

    def __init__(self, config: dict):
        """Inicializa o conector com credenciais EVA.

        Args:
            config: Dict com EVA_SERVER, EVA_DATABASE, EVA_USER,
                    EVA_PASSWORD, EVA_VIEW.

        Raises:
            ValueError: Se credenciais obrigatórias estiverem faltando.
        """
        faltando = [
            k for k in REQUIRED_KEYS
            if not config.get(k)
        ]
        if faltando:
            raise ValueError(
                f"Credenciais EVA obrigatórias faltando: "
                f"{', '.join(faltando)}. "
                f"Defina no .env ou config.py."
            )

        self.server = config["EVA_SERVER"]
        self.database = config.get("EVA_DATABASE", "eva_activities")
        self.user = config["EVA_USER"]
        self.password = config["EVA_PASSWORD"]
        self.view = config.get(
            "EVA_VIEW", "eva_activities.dbo.vwSales"
        )
        self.batch_size = config.get("BATCH_SIZE", BATCH_SIZE)
        self._conn = None
        self._driver = None

        logger.info(
            "ConectorEVA inicializado: server=%s, db=%s, user=%s",
            self.server, self.database, self.user,
        )

    def conectar(self) -> bool:
        """Estabelece conexão com o SQL Server EVA.

        Tenta pymssql primeiro, fallback para pyodbc.

        Returns:
            True se conectou, False se falhou (erro logado).
        """
        # Tentar pymssql primeiro
        if self._tentar_pymssql():
            return True

        # Fallback para pyodbc
        if self._tentar_pyodbc():
            return True

        logger.error(
            "Falha ao conectar ao EVA. "
            "Instale pymssql (pip install pymssql) ou "
            "pyodbc (pip install pyodbc) com driver ODBC."
        )
        return False

    def _tentar_pymssql(self) -> bool:
        """Tenta conexão via pymssql."""
        try:
            import pymssql
            self._conn = pymssql.connect(
                server=self.server,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            self._driver = "pymssql"
            logger.info("Conectado ao EVA via pymssql")
            return True
        except ImportError:
            logger.debug("pymssql não disponível, tentando pyodbc")
            return False
        except Exception as e:
            logger.warning("Falha pymssql: %s", e)
            return False

    def _tentar_pyodbc(self) -> bool:
        """Tenta conexão via pyodbc."""
        try:
            import pyodbc
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.user};"
                f"PWD={self.password}"
            )
            self._conn = pyodbc.connect(conn_str)
            self._driver = "pyodbc"
            logger.info("Conectado ao EVA via pyodbc")
            return True
        except ImportError:
            logger.debug("pyodbc não disponível")
            return False
        except Exception as e:
            logger.warning("Falha pyodbc: %s", e)
            return False

    def coletar_vendas(self, db_manager) -> Dict[str, Any]:
        """Executa query na vwSales e insere no banco V2.

        Args:
            db_manager: Instância de DatabaseManagerV2 (duck typing).

        Returns:
            Dict com estatísticas:
            {lote_id, total, inseridos, erros}
        """
        stats = {
            "lote_id": None,
            "total": 0,
            "inseridos": 0,
            "erros": 0,
        }

        if not self._conn:
            logger.error(
                "Sem conexão EVA. Chame conectar() primeiro."
            )
            return stats

        try:
            cursor = self._conn.cursor()
            logger.info("Executando query na %s...", self.view)
            cursor.execute(EVA_QUERY)

            # Obter nomes das colunas do resultado
            colunas = [
                desc[0] for desc in cursor.description
            ]

            # Coletar todos os registros
            registros = cursor.fetchall()
            stats["total"] = len(registros)
            logger.info(
                "Query retornou %d registros", stats["total"]
            )

            if stats["total"] == 0:
                return stats

            # Gerar hash SHA-256 dos dados para o lote
            hash_dados = self._gerar_hash(registros, colunas)

            # Criar lote de importação
            lote_id = db_manager.criar_lote(
                nome_arquivo=f"eva_vwSales_{datetime.now():%Y%m%d_%H%M%S}",
                tipo="vendas_eva",
                hash_sha256=hash_dados,
            )
            stats["lote_id"] = lote_id

            # Inserir em lotes
            for i in range(0, len(registros), self.batch_size):
                lote_registros = registros[i:i + self.batch_size]
                for row in lote_registros:
                    try:
                        dados = self._mapear_registro(
                            dict(zip(colunas, row))
                        )
                        db_manager.inserir_registro(
                            tabela="vendas_eva",
                            dados=dados,
                            lote_id=lote_id,
                        )
                        stats["inseridos"] += 1
                    except Exception as e:
                        stats["erros"] += 1
                        logger.error(
                            "Erro ao inserir registro: %s", e
                        )

                logger.debug(
                    "Lote %d-%d processado (%d/%d)",
                    i, i + len(lote_registros),
                    stats["inseridos"], stats["total"],
                )

            # Finalizar lote
            db_manager.finalizar_lote(
                lote_id=lote_id,
                qtd_inseridos=stats["inseridos"],
                qtd_erros=stats["erros"],
                status="concluido" if stats["erros"] == 0 else "erro",
            )

            logger.info(
                "Coleta EVA finalizada: lote=%d, total=%d, "
                "inseridos=%d, erros=%d",
                lote_id, stats["total"],
                stats["inseridos"], stats["erros"],
            )

        except Exception as e:
            logger.error("Erro na coleta EVA: %s", e)
            if stats["lote_id"]:
                try:
                    db_manager.finalizar_lote(
                        lote_id=stats["lote_id"],
                        qtd_inseridos=stats["inseridos"],
                        qtd_erros=stats["erros"],
                        status="erro",
                    )
                except Exception:
                    pass

        return stats

    def desconectar(self):
        """Fecha a conexão com o EVA."""
        if self._conn:
            try:
                self._conn.close()
                logger.info(
                    "Conexão EVA fechada (driver=%s)",
                    self._driver,
                )
            except Exception as e:
                logger.warning(
                    "Erro ao fechar conexão EVA: %s", e
                )
            finally:
                self._conn = None
                self._driver = None

    def _mapear_registro(
        self, row: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Mapeia um registro EVA para o formato da tabela vendas_eva.

        Campos dedicados vão em colunas próprias, demais vão em dados_json.

        Args:
            row: Dict com colunas originais do EVA.

        Returns:
            Dict pronto para inserção na tabela vendas_eva.
        """
        dados = {}
        campos_restantes = {}

        for col_eva, valor in row.items():
            col_db = CAMPOS_DEDICADOS.get(col_eva)
            if col_db:
                dados[col_db] = (
                    str(valor).strip() if valor is not None else None
                )
            else:
                campos_restantes[col_eva] = (
                    str(valor).strip() if valor is not None else None
                )

        # Campos restantes vão em dados_json
        dados["dados_json"] = json.dumps(
            campos_restantes, ensure_ascii=False
        )

        return dados

    @staticmethod
    def _gerar_hash(
        registros: list, colunas: list
    ) -> str:
        """Gera hash SHA-256 dos dados coletados.

        Args:
            registros: Lista de tuplas com os dados.
            colunas: Lista de nomes das colunas.

        Returns:
            Hash SHA-256 como string hexadecimal.
        """
        hasher = hashlib.sha256()
        hasher.update(
            json.dumps(colunas, ensure_ascii=False).encode()
        )
        for row in registros:
            hasher.update(
                json.dumps(
                    [str(v) if v is not None else "" for v in row],
                    ensure_ascii=False,
                ).encode()
            )
        return hasher.hexdigest()
