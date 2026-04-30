"""
Processador de arquivos CSV de retorno RPA TIM Pré/Controle.

Lê CSVs com auto-detecção de encoding e delimitador, classifica
cada registro via ClassificadorStatus e insere no banco V2.
"""

import csv
import hashlib
import io
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict

from config import BATCH_SIZE

logger = logging.getLogger(__name__)

# Mapeamento de nomes de coluna normalizados → campo interno
_COLUMN_MAP = {
    "numero_de_acesso": "numero_acesso",
    "codigo_externo": "codigo_externo",
    "protocolo": "protocolo",
    "motivo_de_nao_ter_sido_migrado": "motivo_nao_migrado",
    "data_inicial_do_processamento": "data_inicial_processamento",
    "data_final_do_processamento": "data_final_processamento",
}


def _normalizar_nome_coluna(nome: str) -> str:
    """Remove acentos, converte para minúsculas e substitui espaços/pontuação por _."""
    s = str(nome).strip()
    # Decompor unicode e remover marcas de acento
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s


class ProcessadorRetornoRPA:
    """Processa CSVs de retorno RPA com auto-detecção de encoding/delimitador."""

    def __init__(self, classificador, batch_size: int = BATCH_SIZE):
        """
        Args:
            classificador: Instância de ClassificadorStatus.
            batch_size: Tamanho do lote para inserção no banco.
        """
        self.classificador = classificador
        self.batch_size = batch_size

    # ------------------------------------------------------------------
    # Detecção de encoding
    # ------------------------------------------------------------------

    @staticmethod
    def _detectar_encoding(caminho: str) -> str:
        """Detecta encoding do arquivo tentando UTF-8, UTF-8-BOM, Latin-1, CP1252.

        Args:
            caminho: Caminho para o arquivo CSV.

        Returns:
            Nome do encoding detectado.
        """
        path = Path(caminho)
        raw = path.read_bytes()

        # UTF-8 BOM
        if raw[:3] == b"\xef\xbb\xbf":
            return "utf-8-sig"

        # Tentar UTF-8 estrito
        try:
            raw.decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass

        # Tentar CP1252 (superset de Latin-1 com mais caracteres)
        try:
            raw.decode("cp1252")
            return "cp1252"
        except (UnicodeDecodeError, LookupError):
            pass

        # Fallback Latin-1 (nunca falha)
        return "latin-1"

    # ------------------------------------------------------------------
    # Detecção de delimitador
    # ------------------------------------------------------------------

    @staticmethod
    def _detectar_delimitador(conteudo: str) -> str:
        """Detecta delimitador do CSV: pipe, ponto-e-vírgula ou vírgula.

        Segue o padrão de processar_tim_pre_controle.py.

        Args:
            conteudo: Conteúdo do arquivo como string.

        Returns:
            Caractere delimitador detectado.
        """
        first_line = conteudo.split("\n")[0] if conteudo else ""
        count_pipe = first_line.count("|")
        count_semi = first_line.count(";")
        count_comma = first_line.count(",")

        if count_pipe > count_comma and count_pipe > count_semi:
            return "|"
        if count_semi > count_comma:
            return ";"
        return ","

    # ------------------------------------------------------------------
    # Extração de campos
    # ------------------------------------------------------------------

    @staticmethod
    def _extrair_campos(row: dict) -> Dict[str, str]:
        """Extrai campos relevantes do CSV com normalização de nomes de coluna.

        Args:
            row: Dicionário com chaves sendo os nomes originais das colunas.

        Returns:
            Dicionário com campos normalizados para inserção no banco.
        """
        # Criar mapa normalizado das colunas presentes na row
        norm_map: Dict[str, str] = {}
        for original_key, value in row.items():
            norm_key = _normalizar_nome_coluna(original_key)
            norm_map[norm_key] = str(value).strip() if value else ""

        campos: Dict[str, str] = {}
        for norm_key, campo_interno in _COLUMN_MAP.items():
            campos[campo_interno] = norm_map.get(norm_key, "")

        # data_aprovacao = Data final do processamento (conclusão)
        campos["data_aprovacao"] = campos.get(
            "data_final_processamento", ""
        )

        return campos

    # ------------------------------------------------------------------
    # Processamento principal
    # ------------------------------------------------------------------

    def processar_arquivo(
        self, caminho: str, db_manager
    ) -> Dict[str, Any]:
        """Processa um CSV de retorno RPA.

        Fluxo: detectar encoding → detectar delimitador → ler CSV →
        classificar cada linha via ClassificadorStatus → inserir em
        retornos_rpa_tim em lotes.

        Args:
            caminho: Caminho para o arquivo CSV.
            db_manager: Instância com métodos criar_lote, inserir_registro,
                        finalizar_lote (duck typing).

        Returns:
            Estatísticas {lote_id, total, inseridos, erros}.
        """
        stats: Dict[str, Any] = {
            "lote_id": None,
            "total": 0,
            "inseridos": 0,
            "erros": 0,
        }

        path = Path(caminho)
        if not path.exists():
            logger.error("Arquivo não encontrado: %s", caminho)
            return stats

        # Hash SHA-256 do arquivo
        file_hash = hashlib.sha256(path.read_bytes()).hexdigest()

        # Detectar encoding e ler conteúdo
        encoding = self._detectar_encoding(caminho)
        try:
            content = path.read_text(encoding=encoding, errors="replace")
        except Exception:
            logger.exception("Erro ao ler arquivo %s", path.name)
            return stats

        if not content or not content.strip():
            logger.warning("Arquivo vazio: %s", path.name)
            return stats

        # Detectar delimitador
        delimiter = self._detectar_delimitador(content)

        # Ler CSV
        reader = csv.DictReader(
            io.StringIO(content), delimiter=delimiter
        )

        if not reader.fieldnames:
            logger.warning(
                "Arquivo sem cabeçalho válido: %s", path.name
            )
            return stats

        # Criar lote de importação
        lote_id = db_manager.criar_lote(
            path.name, "retorno_rpa_tim", file_hash
        )
        stats["lote_id"] = lote_id

        origem_arquivo = path.name
        batch: list = []

        for line_num, row in enumerate(reader, start=2):
            stats["total"] += 1
            try:
                campos = self._extrair_campos(row)

                if not campos.get("numero_acesso"):
                    logger.error(
                        "Linha %d sem numero_acesso em %s",
                        line_num,
                        origem_arquivo,
                    )
                    stats["erros"] += 1
                    continue

                # Classificar status
                protocolo = campos.get("protocolo", "")
                motivo = campos.get("motivo_nao_migrado", "")
                status = self.classificador.classificar(
                    protocolo, motivo
                )

                registro = {
                    "numero_acesso": campos["numero_acesso"],
                    "codigo_externo": campos["codigo_externo"],
                    "protocolo": protocolo,
                    "motivo_nao_migrado": motivo,
                    "data_inicial_processamento": campos[
                        "data_inicial_processamento"
                    ],
                    "data_final_processamento": campos[
                        "data_final_processamento"
                    ],
                    "data_aprovacao": campos["data_aprovacao"],
                    "status_classificado": status,
                    "origem_arquivo": origem_arquivo,
                }

                batch.append(registro)

                if len(batch) >= self.batch_size:
                    self._inserir_lote(
                        batch, db_manager, lote_id, stats
                    )
                    batch = []

            except Exception:
                logger.exception(
                    "Erro na linha %d de %s",
                    line_num,
                    origem_arquivo,
                )
                stats["erros"] += 1

        # Inserir registros restantes
        if batch:
            self._inserir_lote(batch, db_manager, lote_id, stats)

        # Finalizar lote
        status_lote = (
            "concluido" if stats["erros"] == 0 else "concluido_com_erros"
        )
        db_manager.finalizar_lote(
            lote_id, stats["inseridos"], stats["erros"], status_lote
        )

        logger.info(
            "Processado %s: %d inseridos, %d erros de %d total",
            origem_arquivo,
            stats["inseridos"],
            stats["erros"],
            stats["total"],
        )

        return stats

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _inserir_lote(
        batch: list,
        db_manager,
        lote_id: int,
        stats: Dict[str, Any],
    ):
        """Insere um lote de registros no banco."""
        for registro in batch:
            try:
                db_manager.inserir_registro(
                    "retornos_rpa_tim", registro, lote_id
                )
                stats["inseridos"] += 1
            except Exception:
                logger.exception(
                    "Erro ao inserir registro: %s",
                    registro.get("codigo_externo", "?"),
                )
                stats["erros"] += 1
