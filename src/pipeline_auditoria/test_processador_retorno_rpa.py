"""
Testes unitários para ProcessadorRetornoRPA.
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.pipeline_auditoria.classificador_status import ClassificadorStatus
from src.pipeline_auditoria.processador_retorno_rpa import (
    ProcessadorRetornoRPA,
    _normalizar_nome_coluna,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def classificador():
    return ClassificadorStatus()


@pytest.fixture
def processador(classificador):
    return ProcessadorRetornoRPA(classificador, batch_size=10)


@pytest.fixture
def db_manager():
    """Mock do db_manager com interface duck-typed."""
    mock = MagicMock()
    mock.criar_lote.return_value = 1
    mock.inserir_registro.return_value = 1
    return mock


def _criar_csv(conteudo: str, encoding: str = "utf-8", suffix=".csv"):
    """Cria arquivo CSV temporário e retorna o caminho."""
    tmp = tempfile.NamedTemporaryFile(
        mode="wb", suffix=suffix, delete=False
    )
    tmp.write(conteudo.encode(encoding))
    tmp.close()
    return tmp.name


# ------------------------------------------------------------------
# Testes de _normalizar_nome_coluna
# ------------------------------------------------------------------

class TestNormalizarNomeColuna:
    def test_acentos_removidos(self):
        assert _normalizar_nome_coluna("Número de acesso") == "numero_de_acesso"

    def test_espacos_viram_underscore(self):
        assert _normalizar_nome_coluna("Código externo") == "codigo_externo"

    def test_caracteres_especiais(self):
        result = _normalizar_nome_coluna(
            "Motivo de não ter sido migrado"
        )
        assert result == "motivo_de_nao_ter_sido_migrado"

    def test_string_vazia(self):
        assert _normalizar_nome_coluna("") == ""

    def test_ja_normalizado(self):
        assert _normalizar_nome_coluna("protocolo") == "protocolo"


# ------------------------------------------------------------------
# Testes de _detectar_encoding
# ------------------------------------------------------------------

class TestDetectarEncoding:
    def test_utf8(self, processador):
        path = _criar_csv("col1;col2\nval1;val2", encoding="utf-8")
        try:
            enc = processador._detectar_encoding(path)
            assert enc == "utf-8"
        finally:
            os.unlink(path)

    def test_utf8_bom(self, processador):
        tmp = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        )
        tmp.write(b"\xef\xbb\xbf" + "col1;col2\n".encode("utf-8"))
        tmp.close()
        try:
            enc = processador._detectar_encoding(tmp.name)
            assert enc == "utf-8-sig"
        finally:
            os.unlink(tmp.name)

    def test_latin1_fallback(self, processador):
        tmp = tempfile.NamedTemporaryFile(
            mode="wb", suffix=".csv", delete=False
        )
        # Bytes inválidos em UTF-8 mas válidos em Latin-1/CP1252
        tmp.write(b"N\xfamero;C\xf3digo\nval1;val2")
        tmp.close()
        try:
            enc = processador._detectar_encoding(tmp.name)
            assert enc in ("cp1252", "latin-1")
        finally:
            os.unlink(tmp.name)


# ------------------------------------------------------------------
# Testes de _detectar_delimitador
# ------------------------------------------------------------------

class TestDetectarDelimitador:
    def test_ponto_e_virgula(self, processador):
        content = "col1;col2;col3\nval1;val2;val3"
        assert processador._detectar_delimitador(content) == ";"

    def test_virgula(self, processador):
        content = "col1,col2,col3\nval1,val2,val3"
        assert processador._detectar_delimitador(content) == ","

    def test_pipe(self, processador):
        content = "col1|col2|col3\nval1|val2|val3"
        assert processador._detectar_delimitador(content) == "|"

    def test_conteudo_vazio(self, processador):
        assert processador._detectar_delimitador("") == ","


# ------------------------------------------------------------------
# Testes de _extrair_campos
# ------------------------------------------------------------------

class TestExtrairCampos:
    def test_campos_com_acentos(self, processador):
        row = {
            "Número de acesso": "11999887766",
            "Código externo": "ABC123",
            "Protocolo": "PROT-001",
            "Motivo de não ter sido migrado": "Score insuficiente, score: 50",
            "Data inicial do processamento": "01/01/2026",
            "Data final do processamento": "02/01/2026",
        }
        campos = processador._extrair_campos(row)
        assert campos["numero_acesso"] == "11999887766"
        assert campos["codigo_externo"] == "ABC123"
        assert campos["protocolo"] == "PROT-001"
        assert campos["motivo_nao_migrado"] == "Score insuficiente, score: 50"
        assert campos["data_inicial_processamento"] == "01/01/2026"
        assert campos["data_final_processamento"] == "02/01/2026"
        assert campos["data_aprovacao"] == "02/01/2026"

    def test_campos_ausentes(self, processador):
        row = {"Código externo": "XYZ"}
        campos = processador._extrair_campos(row)
        assert campos["codigo_externo"] == "XYZ"
        assert campos["numero_acesso"] == ""
        assert campos["protocolo"] == ""


# ------------------------------------------------------------------
# Testes de processar_arquivo
# ------------------------------------------------------------------

class TestProcessarArquivo:
    def test_csv_semicolon_utf8(self, processador, db_manager):
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;PROT-1;;"
            "01/01/2026;02/01/2026\n"
            "11999003344;COD002;;Score insuficiente, score: 30;"
            "03/01/2026;04/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["total"] == 2
            assert stats["inseridos"] == 2
            assert stats["erros"] == 0
            assert stats["lote_id"] == 1
            db_manager.criar_lote.assert_called_once()
            db_manager.finalizar_lote.assert_called_once()
            assert db_manager.inserir_registro.call_count == 2
        finally:
            os.unlink(path)

    def test_csv_comma_delimiter(self, processador, db_manager):
        csv_content = (
            "Número de acesso,Código externo,Protocolo,"
            "Motivo de não ter sido migrado,"
            "Data inicial do processamento,"
            "Data final do processamento\n"
            "11999001122,COD001,PROT-1,,"
            "01/01/2026,02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["total"] == 1
            assert stats["inseridos"] == 1
        finally:
            os.unlink(path)

    def test_arquivo_vazio(self, processador, db_manager):
        path = _criar_csv("", encoding="utf-8")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["total"] == 0
            assert stats["inseridos"] == 0
            assert stats["lote_id"] is None
        finally:
            os.unlink(path)

    def test_arquivo_sem_cabecalho_valido(self, processador, db_manager):
        path = _criar_csv("\n\n", encoding="utf-8")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["inseridos"] == 0
        finally:
            os.unlink(path)

    def test_arquivo_inexistente(self, processador, db_manager):
        stats = processador.processar_arquivo(
            "/tmp/nao_existe_xyz.csv", db_manager
        )
        assert stats["total"] == 0
        assert stats["lote_id"] is None

    def test_linha_sem_numero_acesso(self, processador, db_manager):
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            ";COD001;PROT-1;;"
            "01/01/2026;02/01/2026\n"
            "11999003344;COD002;;motivo qualquer;"
            "03/01/2026;04/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["total"] == 2
            assert stats["inseridos"] == 1
            assert stats["erros"] == 1
        finally:
            os.unlink(path)

    def test_classificacao_emitida(self, processador, db_manager):
        """Registro com protocolo deve ser classificado como Emitida."""
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;PROT-123;Score insuficiente, score: 30;"
            "01/01/2026;02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            processador.processar_arquivo(path, db_manager)
            call_args = db_manager.inserir_registro.call_args
            registro = call_args[0][1]
            assert registro["status_classificado"] == "Emitida"
        finally:
            os.unlink(path)

    def test_classificacao_por_motivo(self, processador, db_manager):
        """Registro sem protocolo deve ser classificado pelo motivo."""
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;;Score insuficiente, score: 30;"
            "01/01/2026;02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            processador.processar_arquivo(path, db_manager)
            call_args = db_manager.inserir_registro.call_args
            registro = call_args[0][1]
            assert registro["status_classificado"] == "LIMITE DE CREDITO"
        finally:
            os.unlink(path)

    def test_latin1_encoding(self, processador, db_manager):
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;PROT-1;;"
            "01/01/2026;02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="latin-1")
        try:
            stats = processador.processar_arquivo(path, db_manager)
            assert stats["inseridos"] == 1
        finally:
            os.unlink(path)

    def test_lote_finalizado_com_status_correto(
        self, processador, db_manager
    ):
        """Lote sem erros deve ter status 'concluido'."""
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;PROT-1;;"
            "01/01/2026;02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            processador.processar_arquivo(path, db_manager)
            call_args = db_manager.finalizar_lote.call_args
            assert call_args[0][3] == "concluido"
        finally:
            os.unlink(path)

    def test_origem_arquivo_registrada(self, processador, db_manager):
        csv_content = (
            "Número de acesso;Código externo;Protocolo;"
            "Motivo de não ter sido migrado;"
            "Data inicial do processamento;"
            "Data final do processamento\n"
            "11999001122;COD001;PROT-1;;"
            "01/01/2026;02/01/2026\n"
        )
        path = _criar_csv(csv_content, encoding="utf-8")
        try:
            processador.processar_arquivo(path, db_manager)
            call_args = db_manager.inserir_registro.call_args
            registro = call_args[0][1]
            assert registro["origem_arquivo"] == Path(path).name
        finally:
            os.unlink(path)
