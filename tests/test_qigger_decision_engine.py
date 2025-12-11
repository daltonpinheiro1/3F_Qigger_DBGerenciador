"""
Testes unitários para a QiggerDecisionEngine
Testa todas as 23 regras de decisão
"""
import pytest
from datetime import datetime
from unittest.mock import Mock

from src.engine import QiggerDecisionEngine, DecisionResult
from src.models.portabilidade import (
    PortabilidadeRecord,
    PortabilidadeStatus,
    StatusOrdem
)


class TestQiggerDecisionEngine:
    """Testes para a QiggerDecisionEngine"""
    
    @pytest.fixture
    def engine(self):
        """Fixture para criar uma instância da engine"""
        return QiggerDecisionEngine()
    
    @pytest.fixture
    def base_record(self):
        """Fixture para criar um registro base válido"""
        return PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            registro_valido=True,
            numero_acesso_valido=True
        )
    
    # ========== TESTES DE VALIDAÇÃO ==========
    
    def test_rule_01_validar_cpf_valido(self, engine, base_record):
        """Teste Rule 1: CPF válido"""
        result = engine._rule_01_validar_cpf(base_record)
        assert result is None
    
    def test_rule_01_validar_cpf_invalido_tamanho(self, engine, base_record):
        """Teste Rule 1: CPF inválido - tamanho incorreto"""
        base_record.cpf = "123456789"
        result = engine._rule_01_validar_cpf(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert "CPF inválido" in result.details
    
    def test_rule_01_validar_cpf_invalido_alfanumerico(self, engine, base_record):
        """Teste Rule 1: CPF inválido - alfanumérico"""
        base_record.cpf = "1234567890A"
        result = engine._rule_01_validar_cpf(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
    
    def test_rule_02_validar_numero_acesso_valido(self, engine, base_record):
        """Teste Rule 2: Número de acesso válido"""
        result = engine._rule_02_validar_numero_acesso(base_record)
        assert result is None
    
    def test_rule_02_validar_numero_acesso_curto(self, engine, base_record):
        """Teste Rule 2: Número de acesso muito curto"""
        base_record.numero_acesso = "1198765"
        result = engine._rule_02_validar_numero_acesso(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert "11 caracteres" in result.details
    
    def test_rule_02_validar_numero_acesso_vazio(self, engine, base_record):
        """Teste Rule 2: Número de acesso vazio"""
        base_record.numero_acesso = ""
        result = engine._rule_02_validar_numero_acesso(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
    
    def test_rule_03_validar_campos_obrigatorios_completos(self, engine, base_record):
        """Teste Rule 3: Todos os campos obrigatórios presentes"""
        result = engine._rule_03_validar_campos_obrigatorios(base_record)
        assert result is None
    
    def test_rule_03_validar_campos_obrigatorios_faltando(self, engine, base_record):
        """Teste Rule 3: Campos obrigatórios faltando"""
        base_record.cpf = ""
        result = engine._rule_03_validar_campos_obrigatorios(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert "CPF" in result.details
    
    def test_rule_22_validar_datas_consistentes(self, engine, base_record):
        """Teste Rule 22: Datas consistentes"""
        base_record.data_portabilidade = datetime(2025, 12, 10)
        base_record.data_final_processamento = datetime(2025, 12, 11)
        result = engine._rule_22_validar_datas(base_record)
        assert result is None
    
    def test_rule_22_validar_datas_inconsistentes(self, engine, base_record):
        """Teste Rule 22: Datas inconsistentes"""
        base_record.data_portabilidade = datetime(2025, 12, 11)
        base_record.data_final_processamento = datetime(2025, 12, 10)
        result = engine._rule_22_validar_datas(base_record)
        assert result is not None
        assert result.decision == "AVISAR"
    
    # ========== TESTES DE STATUS ==========
    
    def test_rule_04_cliente_sem_cadastro(self, engine, base_record):
        """Teste Rule 4: Cliente sem cadastro"""
        base_record.motivo_nao_consultado = "Cliente sem cadastro"
        result = engine._rule_04_cliente_sem_cadastro(base_record)
        assert result is not None
        assert result.decision == "PENDENTE"
        assert "cadastro" in result.details.lower()
    
    def test_rule_05_portabilidade_cancelada(self, engine, base_record):
        """Teste Rule 5: Portabilidade cancelada"""
        base_record.status_bilhete = PortabilidadeStatus.CANCELADA
        base_record.motivo_cancelamento = "Cancelamento pelo Cliente"
        result = engine._rule_05_portabilidade_cancelada(base_record)
        assert result is not None
        assert result.decision == "CANCELAR"
        assert result.priority == 2
    
    def test_rule_06_portabilidade_pendente(self, engine, base_record):
        """Teste Rule 6: Portabilidade pendente"""
        base_record.status_bilhete = PortabilidadeStatus.PENDENTE
        base_record.data_portabilidade = datetime(2025, 12, 15)
        result = engine._rule_06_portabilidade_pendente(base_record)
        assert result is not None
        assert result.decision == "MONITORAR"
        assert result.priority == 4
    
    def test_rule_07_portabilidade_concluida(self, engine, base_record):
        """Teste Rule 7: Portabilidade concluída"""
        base_record.status_bilhete = PortabilidadeStatus.CONCLUIDA
        base_record.data_portabilidade = datetime(2025, 12, 10)
        result = engine._rule_07_portabilidade_concluida(base_record)
        assert result is not None
        assert result.decision == "CONCLUIR"
        assert result.priority == 2
    
    def test_rule_08_conflito_detectado(self, engine, base_record):
        """Teste Rule 8: Conflito detectado"""
        base_record.status_bilhete = PortabilidadeStatus.CONFLITO
        base_record.operadora_doadora = "VIVO"
        result = engine._rule_08_conflito_detectado(base_record)
        assert result is not None
        assert result.decision == "RESOLVER_CONFLITO"
        assert result.priority == 2
    
    def test_rule_09_falha_parcial(self, engine, base_record):
        """Teste Rule 9: Falha parcial"""
        base_record.status_bilhete = PortabilidadeStatus.FALHA_PARCIAL
        base_record.data_conclusao_ordem = datetime(2025, 12, 10)
        result = engine._rule_09_falha_parcial(base_record)
        assert result is not None
        assert result.decision == "REPROCESSAR"
        assert result.priority == 3
    
    def test_rule_10_erro_aprovisionamento(self, engine, base_record):
        """Teste Rule 10: Erro no aprovisionamento"""
        base_record.status_ordem = StatusOrdem.ERRO_APROVISIONAMENTO
        result = engine._rule_10_erro_aprovisionamento(base_record)
        assert result is not None
        assert result.decision == "CORRIGIR_APROVISIONAMENTO"
        assert result.priority == 2
    
    def test_rule_11_erro_sistema(self, engine, base_record):
        """Teste Rule 11: Erro do sistema"""
        base_record.motivo_nao_consultado = "[Sistema] Não foi possível processar esse registro."
        result = engine._rule_11_erro_sistema(base_record)
        assert result is not None
        assert result.decision == "REPROCESSAR"
        assert result.priority == 2
    
    def test_rule_12_sem_bilhete_portabilidade(self, engine, base_record):
        """Teste Rule 12: Sem bilhete de portabilidade"""
        base_record.motivo_nao_consultado = "Nenhum bilhete de portabilidade encontrado"
        result = engine._rule_12_sem_bilhete_portabilidade(base_record)
        assert result is not None
        assert result.decision == "PESQUISAR"
        assert result.priority == 4
    
    def test_rule_18_portabilidade_suspensa(self, engine, base_record):
        """Teste Rule 18: Portabilidade suspensa"""
        base_record.status_bilhete = PortabilidadeStatus.SUSPENSA
        base_record.data_portabilidade = datetime(2025, 12, 10)
        result = engine._rule_18_portabilidade_suspensa(base_record)
        assert result is not None
        assert result.decision == "INVESTIGAR"
        assert result.priority == 3
    
    def test_rule_19_ordem_concluida(self, engine, base_record):
        """Teste Rule 19: Ordem concluída"""
        base_record.status_ordem = StatusOrdem.CONCLUIDO
        base_record.preco_ordem = "R$29,99"
        base_record.data_conclusao_ordem = datetime(2025, 12, 10)
        result = engine._rule_19_ordem_concluida(base_record)
        assert result is not None
        assert result.decision == "ARQUIVAR"
        assert result.priority == 5
    
    def test_rule_20_ordem_pendente(self, engine, base_record):
        """Teste Rule 20: Ordem pendente"""
        base_record.status_ordem = StatusOrdem.PENDENTE
        result = engine._rule_20_ordem_pendente(base_record)
        assert result is not None
        assert result.decision == "AGUARDAR"
        assert result.priority == 4
    
    def test_rule_21_em_aprovisionamento(self, engine, base_record):
        """Teste Rule 21: Em aprovisionamento"""
        base_record.status_ordem = StatusOrdem.EM_APROVISIONAMENTO
        result = engine._rule_21_em_aprovisionamento(base_record)
        assert result is not None
        assert result.decision == "MONITORAR"
        assert result.priority == 4
    
    # ========== TESTES DE MOTIVOS ==========
    
    def test_rule_13_motivo_recusa_cliente(self, engine, base_record):
        """Teste Rule 13: Rejeição do cliente via SMS"""
        base_record.motivo_recusa = "Rejeição do Cliente via SMS"
        result = engine._rule_13_motivo_recusa_cliente(base_record)
        assert result is not None
        assert result.decision == "CANCELAR"
        assert result.priority == 2
    
    def test_rule_14_motivo_cancelamento_automatico(self, engine, base_record):
        """Teste Rule 14: Cancelamento automático pela BDR"""
        base_record.motivo_cancelamento = "Cancelamento Automático pela BDR"
        result = engine._rule_14_motivo_cancelamento_automatico(base_record)
        assert result is not None
        assert result.decision == "CANCELAR"
        assert result.priority == 2
    
    def test_rule_15_cpf_invalido(self, engine, base_record):
        """Teste Rule 15: CPF inválido"""
        base_record.motivo_recusa = "CPF Inválido"
        result = engine._rule_15_cpf_invalido(base_record)
        assert result is not None
        assert result.decision == "CORRIGIR"
        assert result.priority == 2
    
    def test_rule_16_numero_vago(self, engine, base_record):
        """Teste Rule 16: Portabilidade de número vago"""
        base_record.motivo_recusa = "Portabillidade de Número Vago"
        result = engine._rule_16_numero_vago(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert result.priority == 2
    
    def test_rule_17_sem_resposta_sms(self, engine, base_record):
        """Teste Rule 17: Sem resposta do SMS"""
        base_record.motivo_recusa = "Sem Resposta do SMS do Cliente"
        result = engine._rule_17_sem_resposta_sms(base_record)
        assert result is not None
        assert result.decision == "REAGENDAR"
        assert result.priority == 3
    
    # ========== TESTES ESPECIAIS ==========
    
    def test_rule_23_priorizar_ultimo_bilhete(self, engine, base_record):
        """Teste Rule 23: Priorizar último bilhete"""
        base_record.ultimo_bilhete = True
        result = engine._rule_23_priorizar_ultimo_bilhete(base_record)
        assert result is not None
        assert result.decision == "PRIORIZAR"
        assert result.priority == 1  # Máxima prioridade
    
    # ========== TESTES DE INTEGRAÇÃO ==========
    
    def test_process_record_multiplas_regras(self, engine, base_record):
        """Teste: Processar registro com múltiplas regras aplicáveis"""
        base_record.status_bilhete = PortabilidadeStatus.CANCELADA
        base_record.motivo_cancelamento = "Cancelamento pelo Cliente"
        base_record.ultimo_bilhete = True
        
        results = engine.process_record(base_record)
        
        # Deve aplicar múltiplas regras
        assert len(results) > 1
        
        # Verificar que as regras foram aplicadas
        rule_names = [r.rule_name for r in results]
        assert 'rule_05_portabilidade_cancelada' in rule_names
        assert 'rule_23_priorizar_ultimo_bilhete' in rule_names
        
        # Verificar ordenação por prioridade
        priorities = [r.priority for r in results]
        assert priorities == sorted(priorities)
    
    def test_get_applicable_rules(self, engine, base_record):
        """Teste: Obter lista de regras aplicáveis"""
        base_record.status_bilhete = PortabilidadeStatus.CANCELADA
        base_record.ultimo_bilhete = True
        
        applicable = engine.get_applicable_rules(base_record)
        
        assert len(applicable) > 0
        assert 'rule_05_portabilidade_cancelada' in applicable
        assert 'rule_23_priorizar_ultimo_bilhete' in applicable
    
    def test_all_23_rules_registered(self, engine):
        """Teste: Verificar que todas as 23 regras estão registradas"""
        assert len(engine.rules_registry) == 23
        
        expected_rules = [
            'rule_01_validar_cpf',
            'rule_02_validar_numero_acesso',
            'rule_03_validar_campos_obrigatorios',
            'rule_04_cliente_sem_cadastro',
            'rule_05_portabilidade_cancelada',
            'rule_06_portabilidade_pendente',
            'rule_07_portabilidade_concluida',
            'rule_08_conflito_detectado',
            'rule_09_falha_parcial',
            'rule_10_erro_aprovisionamento',
            'rule_11_erro_sistema',
            'rule_12_sem_bilhete_portabilidade',
            'rule_13_motivo_recusa_cliente',
            'rule_14_motivo_cancelamento_automatico',
            'rule_15_cpf_invalido',
            'rule_16_numero_vago',
            'rule_17_sem_resposta_sms',
            'rule_18_portabilidade_suspensa',
            'rule_19_ordem_concluida',
            'rule_20_ordem_pendente',
            'rule_21_em_aprovisionamento',
            'rule_22_validar_datas',
            'rule_23_priorizar_ultimo_bilhete',
        ]
        
        for rule in expected_rules:
            assert rule in engine.rules_registry, f"Regra {rule} não encontrada"

