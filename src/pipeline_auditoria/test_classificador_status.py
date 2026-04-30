"""
Testes unitários para ClassificadorStatus.
"""

import unittest

from config import PARAMETRIZACAO_STATUS
from src.pipeline_auditoria.classificador_status import ClassificadorStatus


class TestClassificadorStatus(unittest.TestCase):
    """Testes unitários para o ClassificadorStatus."""

    def setUp(self):
        self.classificador = ClassificadorStatus()

    # --- Protocolo preenchido → "Emitida" ---

    def test_protocolo_preenchido_retorna_emitida(self):
        resultado = self.classificador.classificar("PROT-123", "qualquer motivo")
        self.assertEqual(resultado, "Emitida")

    def test_protocolo_preenchido_com_motivo_de_regra(self):
        """Protocolo tem prioridade sobre qualquer motivo."""
        motivo = PARAMETRIZACAO_STATUS[0]["padrao"]
        resultado = self.classificador.classificar("PROT-1", motivo)
        self.assertEqual(resultado, "Emitida")

    # --- Protocolo vazio/None → avalia motivo ---

    def test_protocolo_none_avalia_motivo(self):
        motivo = "Score insuficiente, score: 200"
        resultado = self.classificador.classificar(None, motivo)
        self.assertEqual(resultado, "LIMITE DE CREDITO")

    def test_protocolo_vazio_avalia_motivo(self):
        motivo = "Score insuficiente, score: 200"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "LIMITE DE CREDITO")

    def test_protocolo_whitespace_avalia_motivo(self):
        motivo = "Score insuficiente, score: 200"
        resultado = self.classificador.classificar("   ", motivo)
        self.assertEqual(resultado, "LIMITE DE CREDITO")

    # --- Cada regra de parametrização ---

    def test_regra_cliente_ja_migrado(self):
        motivo = "O plano: TIM Controle Ligações Ilimitadas 9 0 já está ativo desde: 2025-01-01"
        resultado = self.classificador.classificar(None, motivo)
        self.assertEqual(resultado, "CLIENTE JÁ MIGRADO")

    def test_regra_limite_credito(self):
        motivo = "Score insuficiente, score: 150"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "LIMITE DE CREDITO")

    def test_regra_falha_processamento_sistema(self):
        motivo = "[Sistema] Não foi possível processar esse registro."
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "FALHA PROCESSAMENTO")

    def test_regra_ordem_processamento(self):
        motivo = "Existe uma ordem Em aprovisionamento para este número"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "ORDEM EM PROCESSAMENTO")

    def test_regra_restricao_interna(self):
        motivo = "Perfil Pré-Pago. Cliente com restrição de administrativa na TIM - bloqueio"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "RESTRICAO INTERNA")

    def test_regra_linha_inativa(self):
        motivo = "O numero de acesso se encontra Cancelado."
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "LINHA INATIVA")

    def test_regra_ddd_fora_estado(self):
        motivo = "Cliente não possui nenhum Billing Profile com endereço correspondente ao DDD selecionado"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "DDD FORA DO ESTADO")

    def test_regra_divergencia_endereco(self):
        motivo = "Endereço divergente do cadastro"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "DIVERGENCIA ENDERECO")

    def test_regra_falha_processamento_siebel(self):
        motivo = "Erro ao executar o sub-processo 'TIM Criar Cliente WF' - detalhes"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "FALHA PROCESSAMENTO")

    def test_regra_cep_nao_encontrado(self):
        motivo = "CEP não encontrado na base"
        resultado = self.classificador.classificar("", motivo)
        self.assertEqual(resultado, "DIVERGENCIA ENDERECO")

    # --- Fallback ---

    def test_fallback_nao_parametrizado(self):
        resultado = self.classificador.classificar("", "motivo desconhecido xyz")
        self.assertEqual(resultado, "NAO PARAMETRIZADO")

    def test_fallback_motivo_vazio(self):
        resultado = self.classificador.classificar("", "")
        self.assertEqual(resultado, "PENDENTE")

    def test_fallback_motivo_none(self):
        resultado = self.classificador.classificar(None, None)
        self.assertEqual(resultado, "PENDENTE")

    # --- Regras customizadas ---

    def test_regras_customizadas(self):
        regras = [{"padrao": "teste", "status": "STATUS_TESTE"}]
        c = ClassificadorStatus(regras=regras)
        self.assertEqual(c.classificar("", "contém teste aqui"), "STATUS_TESTE")

    def test_regras_customizadas_sem_match(self):
        regras = [{"padrao": "teste", "status": "STATUS_TESTE"}]
        c = ClassificadorStatus(regras=regras)
        self.assertEqual(c.classificar("", "sem correspondência"), "NAO PARAMETRIZADO")

    def test_primeira_regra_vence(self):
        """Quando múltiplas regras correspondem, a primeira na ordem vence."""
        regras = [
            {"padrao": "erro", "status": "PRIMEIRO"},
            {"padrao": "erro", "status": "SEGUNDO"},
        ]
        c = ClassificadorStatus(regras=regras)
        self.assertEqual(c.classificar("", "mensagem de erro"), "PRIMEIRO")


if __name__ == "__main__":
    unittest.main()
