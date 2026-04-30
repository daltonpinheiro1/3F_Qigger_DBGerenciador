"""
Testes unitários para PipelineAuditoria.

Usa banco SQLite em memória para validar o cruzamento EVA × RPA
e a orquestração do pipeline.
"""

import pytest
from unittest.mock import patch

from src.database.db_manager_v2 import DatabaseManagerV2
from src.pipeline_auditoria.pipeline import PipelineAuditoria


@pytest.fixture
def db_manager(tmp_path):
    """Cria um DatabaseManagerV2 com banco temporário."""
    db_path = str(tmp_path / "test_pipeline.db")
    return DatabaseManagerV2(db_path)


@pytest.fixture
def pipeline(db_manager):
    """Cria PipelineAuditoria com EVA desabilitado (sem credenciais)."""
    with patch(
        "src.pipeline_auditoria.pipeline.EVA_SERVER", ""
    ), patch(
        "src.pipeline_auditoria.pipeline.EVA_USER", ""
    ), patch(
        "src.pipeline_auditoria.pipeline.EVA_PASSWORD", ""
    ):
        p = PipelineAuditoria(db_manager)
    return p


def _inserir_venda_eva(db_manager, numero_acesso, **kwargs):
    """Helper para inserir uma venda EVA no banco."""
    dados = {
        "numero_acesso": numero_acesso,
        "cod_venda": kwargs.get("cod_venda", "COD001"),
        "operacao": kwargs.get("operacao", "VENDA"),
        "pedido": kwargs.get("pedido", "PED001"),
        "id_atendimento": kwargs.get("id_atendimento", "ATD001"),
        "data_hora_gravacao": kwargs.get("data_hora_gravacao", "2025-01-01"),
        "data_emissao": kwargs.get("data_emissao", "2025-01-02"),
        "nome_cliente": kwargs.get("nome_cliente", "João Silva"),
        "cpf": kwargs.get("cpf", "12345678901"),
        "telefone": kwargs.get("telefone", "11999999999"),
        "produto": kwargs.get("produto", "TIM Controle"),
        "plano": kwargs.get("plano", "Controle 25GB"),
        "status_venda": kwargs.get("status_venda", "Aprovada"),
        "canal": kwargs.get("canal", "Televendas"),
        "equipe": kwargs.get("equipe", "Equipe A"),
        "vendedor": kwargs.get("vendedor", "Vendedor 1"),
        "supervisor": kwargs.get("supervisor", "Supervisor 1"),
        "dados_json": kwargs.get("dados_json", "{}"),
    }
    lote_id = db_manager.criar_lote("test_eva", "vendas_eva", f"hash_eva_{numero_acesso}")
    db_manager.inserir_registro("vendas_eva", dados, lote_id)
    db_manager.finalizar_lote(lote_id, 1, 0, "concluido")
    return lote_id


def _inserir_retorno_rpa(db_manager, numero_acesso, **kwargs):
    """Helper para inserir um retorno RPA no banco."""
    dados = {
        "numero_acesso": numero_acesso,
        "codigo_externo": kwargs.get("codigo_externo", "COD001"),
        "protocolo": kwargs.get("protocolo", ""),
        "motivo_nao_migrado": kwargs.get("motivo_nao_migrado", ""),
        "data_inicial_processamento": kwargs.get("data_inicial_processamento", "2025-01-03"),
        "data_final_processamento": kwargs.get("data_final_processamento", "2025-01-03"),
        "data_aprovacao": kwargs.get("data_aprovacao", "2025-01-03"),
        "status_classificado": kwargs.get("status_classificado", "Emitida"),
        "origem_arquivo": kwargs.get("origem_arquivo", "test.csv"),
    }
    lote_id = db_manager.criar_lote("test_rpa", "retorno_rpa_tim", f"hash_rpa_{numero_acesso}")
    db_manager.inserir_registro("retornos_rpa_tim", dados, lote_id)
    db_manager.finalizar_lote(lote_id, 1, 0, "concluido")
    return lote_id


class TestPipelineInit:
    """Testes de inicialização do PipelineAuditoria."""

    def test_init_sem_credenciais_eva(self, db_manager):
        """Pipeline inicializa sem EVA quando credenciais ausentes."""
        with patch(
            "src.pipeline_auditoria.pipeline.EVA_SERVER", ""
        ), patch(
            "src.pipeline_auditoria.pipeline.EVA_USER", ""
        ), patch(
            "src.pipeline_auditoria.pipeline.EVA_PASSWORD", ""
        ):
            p = PipelineAuditoria(db_manager)
        assert p._conector_eva is None
        assert p.classificador is not None
        assert p.processador_rpa is not None

    def test_init_com_credenciais_eva(self, db_manager):
        """Pipeline inicializa ConectorEVA quando credenciais presentes."""
        with patch.dict(
            "os.environ",
            {
                "EVA_SERVER": "test.server.com",
                "EVA_USER": "user",
                "EVA_PASSWORD": "pass",
            },
        ):
            p = PipelineAuditoria(db_manager)
        assert p._conector_eva is not None


class TestEtapaColeta:
    """Testes da etapa de coleta EVA."""

    def test_coleta_sem_conector(self, pipeline):
        """Retorna stats vazias quando conector não disponível."""
        stats = pipeline._etapa_coleta_eva()
        assert stats["total"] == 0
        assert stats["inseridos"] == 0


class TestEtapaRetornosRPA:
    """Testes da etapa de retornos RPA."""

    def test_retornos_arquivo_inexistente(self, pipeline):
        """Retorna stats zeradas para arquivo inexistente."""
        resultados = pipeline._etapa_retornos_rpa(["/tmp/nao_existe.csv"])
        assert len(resultados) == 1
        assert resultados[0]["inseridos"] == 0

    def test_retornos_lista_vazia(self, pipeline):
        """Retorna lista vazia para nenhum arquivo."""
        resultados = pipeline._etapa_retornos_rpa([])
        assert resultados == []


class TestEtapaCruzamento:
    """Testes da etapa de cruzamento EVA × RPA."""

    def test_cruzamento_vazio(self, pipeline):
        """Sem dados, retorna stats zeradas."""
        stats = pipeline._etapa_cruzamento()
        assert stats["total"] == 0
        assert stats["com_match"] == 0

    def test_cruzamento_com_match(self, pipeline, db_manager):
        """Registros com numero_acesso correspondente geram match."""
        _inserir_venda_eva(db_manager, "11999001122")
        _inserir_retorno_rpa(db_manager, "11999001122", status_classificado="Emitida")

        stats = pipeline._etapa_cruzamento()
        assert stats["com_match"] == 1
        assert stats["eva_sem_rpa"] == 0
        assert stats["rpa_sem_eva"] == 0
        assert stats["inseridos"] == 1

    def test_cruzamento_eva_sem_rpa(self, pipeline, db_manager):
        """EVA sem RPA gera registro com PENDENTE_RETORNO."""
        _inserir_venda_eva(db_manager, "11999002233")

        stats = pipeline._etapa_cruzamento()
        assert stats["eva_sem_rpa"] == 1
        assert stats["distribuicao_status"].get("PENDENTE_RETORNO") == 1

    def test_cruzamento_rpa_sem_eva(self, pipeline, db_manager):
        """RPA sem EVA gera registro com campos EVA vazios."""
        _inserir_retorno_rpa(
            db_manager, "11999003344", status_classificado="FALHA PROCESSAMENTO"
        )

        stats = pipeline._etapa_cruzamento()
        assert stats["rpa_sem_eva"] == 1
        assert stats["distribuicao_status"].get("FALHA PROCESSAMENTO") == 1

    def test_cruzamento_misto(self, pipeline, db_manager):
        """Cenário misto: match + EVA sem RPA + RPA sem EVA."""
        _inserir_venda_eva(db_manager, "11999010010")
        _inserir_venda_eva(db_manager, "11999011011")
        _inserir_retorno_rpa(db_manager, "11999010010", status_classificado="Emitida")
        _inserir_retorno_rpa(db_manager, "11999012012", status_classificado="LINHA INATIVA")

        stats = pipeline._etapa_cruzamento()
        assert stats["com_match"] == 1
        assert stats["eva_sem_rpa"] == 1
        assert stats["rpa_sem_eva"] == 1
        assert stats["total"] == 3
        assert stats["inseridos"] == 3

    def test_cruzamento_dados_persistidos(self, pipeline, db_manager):
        """Verifica que dados do cruzamento são persistidos no banco."""
        _inserir_venda_eva(db_manager, "11999020020", nome_cliente="Maria")
        _inserir_retorno_rpa(
            db_manager, "11999020020",
            protocolo="PROT123",
            status_classificado="Emitida",
        )

        pipeline._etapa_cruzamento()

        registro = db_manager.buscar_corrente(
            "auditoria_vendas_tim", "numero_acesso", "11999020020"
        )
        assert registro is not None
        assert registro["nome_cliente"] == "Maria"
        assert registro["protocolo"] == "PROT123"
        assert registro["status_classificado"] == "Emitida"


class TestExecutar:
    """Testes do método executar (orquestração completa)."""

    def test_executar_sem_dados(self, pipeline):
        """Pipeline executa sem erros mesmo sem dados."""
        stats = pipeline.executar(arquivos_rpa=[])
        assert "eva" in stats
        assert "rpa" in stats
        assert "cruzamento" in stats
        assert "duracao_segundos" in stats

    def test_executar_com_rpa_e_cruzamento(self, pipeline, db_manager):
        """Pipeline completo com dados RPA pré-inseridos."""
        _inserir_venda_eva(db_manager, "11999100100")
        _inserir_retorno_rpa(db_manager, "11999100100", status_classificado="Emitida")

        stats = pipeline.executar(arquivos_rpa=[])
        cruzamento = stats["cruzamento"]
        assert cruzamento["com_match"] == 1
        assert cruzamento["inseridos"] == 1
