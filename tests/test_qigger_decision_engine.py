"""
Testes unitários para a QiggerDecisionEngine
Versão 2.0 - Testa a nova arquitetura com triggers.xlsx
"""
import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import Mock, patch
import pandas as pd

from src.engine import QiggerDecisionEngine, DecisionResult
from src.engine.trigger_loader import TriggerLoader
from src.models.portabilidade import (
    PortabilidadeRecord,
    PortabilidadeStatus,
    StatusOrdem,
    TriggerRule
)


class TestQiggerDecisionEngine:
    """Testes para a QiggerDecisionEngine"""
    
    @pytest.fixture
    def temp_triggers_xlsx(self):
        """Fixture para criar um arquivo triggers.xlsx temporário"""
        # Criar dados de teste
        data = {
            'REGRA_ID': [1, 2, 3],
            'Status do bilhete': ['Portabilidade Cancelada', 'Portado', None],
            'Operadora doadora': ['VIVO', 'CLARO', None],
            'Motivo da recusa': ['Rejeição do Cliente via SMS', None, None],
            'Motivo do cancelamento': ['Rejeição do Cliente via SMS', None, None],
            'Último bilhete de portabilidade?': ['Sim', 'Sim', 'Sim'],
            'Motivo de não ter sido consultado': [None, None, 'Cliente sem cadastro'],
            'Novo status do bilhete': [None, None, None],
            'Ajustes número de acesso': [None, None, None],
            'O que aconteceu': ['CANCELADO A PEDIDO CLIENTE', 'BP FECHADO', 'ERRO SIEBEL'],
            'Ação a ser realizada': ['CANCELADO A PEDIDO CLIENTE', 'POS VENDA PARABENIZAÇÃO', 'VALIDAR GROSS'],
            'Tipo de mensagem': ['NÃO ENVIAR', 'CONFIRMACAO BP', 'LIBERACAO BONUS'],
            'Templete': ['-', 'EM CRIAÇÃO', '2'],
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_path = f.name
        
        df.to_excel(temp_path, index=False)
        yield temp_path
        
        # Limpeza
        os.unlink(temp_path)
    
    @pytest.fixture
    def engine(self, temp_triggers_xlsx):
        """Fixture para criar uma instância da engine com triggers temporário"""
        return QiggerDecisionEngine(triggers_path=temp_triggers_xlsx)
    
    @pytest.fixture
    def base_record(self):
        """Fixture para criar um registro base válido"""
        return PortabilidadeRecord(
            cpf="52998224725",  # CPF válido
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            ultimo_bilhete=True,
            registro_valido=True,
        )
    
    # ========== TESTES DE VALIDAÇÃO ==========
    
    def test_validate_cpf_valido(self, engine, base_record):
        """Teste: CPF válido passa validação"""
        result = engine._validate_cpf(base_record)
        assert result is None
    
    def test_validate_cpf_invalido_tamanho(self, engine, base_record):
        """Teste: CPF inválido - tamanho incorreto"""
        base_record.cpf = "123456789"
        result = engine._validate_cpf(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert "CPF inválido" in result.details
    
    def test_validate_cpf_invalido_digitos(self, engine, base_record):
        """Teste: CPF inválido - dígitos verificadores incorretos"""
        base_record.cpf = "12345678901"  # CPF com dígitos incorretos
        result = engine._validate_cpf(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
    
    def test_validate_numero_acesso_valido(self, engine, base_record):
        """Teste: Número de acesso válido"""
        result = engine._validate_numero_acesso(base_record)
        assert result is None
    
    def test_validate_numero_acesso_curto(self, engine, base_record):
        """Teste: Número de acesso muito curto"""
        base_record.numero_acesso = "1198765"
        result = engine._validate_numero_acesso(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
    
    def test_validate_campos_obrigatorios_completos(self, engine, base_record):
        """Teste: Todos os campos obrigatórios presentes"""
        result = engine._validate_campos_obrigatorios(base_record)
        assert result is None
    
    def test_validate_campos_obrigatorios_faltando(self, engine, base_record):
        """Teste: Campos obrigatórios faltando"""
        base_record.cpf = ""
        result = engine._validate_campos_obrigatorios(base_record)
        assert result is not None
        assert result.decision == "REJEITAR"
        assert "CPF" in result.details
    
    # ========== TESTES DE MATCHING COM TRIGGERS ==========
    
    def test_process_record_com_match(self, engine, base_record):
        """Teste: Processar registro com regra correspondente"""
        base_record.status_bilhete = PortabilidadeStatus.CANCELADA
        base_record.operadora_doadora = "VIVO"
        base_record.motivo_recusa = "Rejeição do Cliente via SMS"
        base_record.motivo_cancelamento = "Rejeição do Cliente via SMS"
        
        results = engine.process_record(base_record, save_to_db=False)
        
        # Deve ter encontrado match
        trigger_results = [r for r in results if r.rule_name.startswith('trigger_rule_')]
        assert len(trigger_results) > 0
        
        # Verificar dados do match
        trigger_result = trigger_results[0]
        assert trigger_result.o_que_aconteceu == "CANCELADO A PEDIDO CLIENTE"
        assert trigger_result.acao_a_realizar == "CANCELADO A PEDIDO CLIENTE"
        assert trigger_result.mapeado is True
    
    def test_process_record_sem_match(self, engine, base_record):
        """Teste: Processar registro sem regra correspondente"""
        base_record.status_bilhete = PortabilidadeStatus.SUSPENSA  # Status não mapeado
        base_record.operadora_doadora = "OPERADORA_DESCONHECIDA"
        
        results = engine.process_record(base_record, save_to_db=False)
        
        # Deve ter resultado como não mapeado
        unmapped_results = [r for r in results if r.rule_name == 'unmapped']
        assert len(unmapped_results) > 0
        
        unmapped = unmapped_results[0]
        assert unmapped.o_que_aconteceu == "NÃO MAPEADO"
        assert unmapped.mapeado is False
    
    def test_process_record_portabilidade_concluida(self, engine, base_record):
        """Teste: Processar registro de portabilidade concluída"""
        base_record.status_bilhete = PortabilidadeStatus.CONCLUIDA
        base_record.operadora_doadora = "CLARO"
        
        results = engine.process_record(base_record, save_to_db=False)
        
        trigger_results = [r for r in results if r.rule_name.startswith('trigger_rule_')]
        assert len(trigger_results) > 0
        
        trigger_result = trigger_results[0]
        assert trigger_result.o_que_aconteceu == "BP FECHADO"
    
    def test_process_record_cliente_sem_cadastro(self, engine, base_record):
        """Teste: Processar registro com erro de cadastro"""
        base_record.motivo_nao_consultado = "Cliente sem cadastro"
        
        results = engine.process_record(base_record, save_to_db=False)
        
        trigger_results = [r for r in results if r.rule_name.startswith('trigger_rule_')]
        assert len(trigger_results) > 0
        
        trigger_result = trigger_results[0]
        assert trigger_result.o_que_aconteceu == "ERRO SIEBEL"
        assert trigger_result.acao_a_realizar == "VALIDAR GROSS"
    
    # ========== TESTES DE DECISION RESULT ==========
    
    def test_decision_result_estrutura(self, engine, base_record):
        """Teste: Estrutura do DecisionResult"""
        base_record.status_bilhete = PortabilidadeStatus.CANCELADA
        base_record.operadora_doadora = "VIVO"
        base_record.motivo_recusa = "Rejeição do Cliente via SMS"
        base_record.motivo_cancelamento = "Rejeição do Cliente via SMS"
        
        results = engine.process_record(base_record, save_to_db=False)
        
        for result in results:
            assert hasattr(result, 'rule_name')
            assert hasattr(result, 'decision')
            assert hasattr(result, 'action')
            assert hasattr(result, 'details')
            assert hasattr(result, 'priority')
            assert hasattr(result, 'regra_id')
            assert hasattr(result, 'o_que_aconteceu')
            assert hasattr(result, 'acao_a_realizar')
            assert hasattr(result, 'tipo_mensagem')
            assert hasattr(result, 'template')
            assert hasattr(result, 'mapeado')
    
    def test_decision_results_ordenados_por_prioridade(self, engine, base_record):
        """Teste: Resultados ordenados por prioridade"""
        base_record.cpf = ""  # Vai gerar erro de validação
        
        results = engine.process_record(base_record, save_to_db=False)
        
        priorities = [r.priority for r in results]
        assert priorities == sorted(priorities)
    
    # ========== TESTES DE INTEGRAÇÃO ==========
    
    def test_get_rules_stats(self, engine):
        """Teste: Obter estatísticas das regras"""
        stats = engine.get_rules_stats()
        
        assert 'total_regras' in stats
        assert stats['total_regras'] == 3  # 3 regras no fixture
    
    def test_get_applicable_rules_preview(self, engine, base_record):
        """Teste: Preview de regra aplicável"""
        base_record.status_bilhete = PortabilidadeStatus.CONCLUIDA
        base_record.operadora_doadora = "CLARO"
        
        rule = engine.get_applicable_rules_preview(base_record)
        
        assert rule is not None
        assert rule.regra_id == 2
        assert rule.o_que_aconteceu == "BP FECHADO"
    
    def test_reload_triggers(self, engine):
        """Teste: Recarregar regras"""
        # Não deve dar erro
        engine.reload_triggers()
        
        stats = engine.get_rules_stats()
        assert stats['total_regras'] == 3


class TestTriggerLoader:
    """Testes para o TriggerLoader"""
    
    @pytest.fixture
    def temp_triggers_xlsx(self):
        """Fixture para criar um arquivo triggers.xlsx temporário"""
        data = {
            'REGRA_ID': [1, 2],
            'Status do bilhete': ['Portabilidade Cancelada', 'Portado'],
            'Operadora doadora': ['VIVO', 'CLARO'],
            'Motivo da recusa': [None, None],
            'Motivo do cancelamento': [None, None],
            'Último bilhete de portabilidade?': ['Sim', 'Sim'],
            'Motivo de não ter sido consultado': [None, None],
            'Novo status do bilhete': [None, None],
            'Ajustes número de acesso': [None, None],
            'O que aconteceu': ['CANCELAMENTO', 'PORTADO'],
            'Ação a ser realizada': ['REABERTURA', 'PARABENIZAÇÃO'],
            'Tipo de mensagem': ['LIBERACAO', 'CONFIRMACAO'],
            'Templete': ['1', '2'],
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_path = f.name
        
        df.to_excel(temp_path, index=False)
        yield temp_path
        
        os.unlink(temp_path)
    
    def test_load_rules(self, temp_triggers_xlsx):
        """Teste: Carregar regras do xlsx"""
        loader = TriggerLoader(temp_triggers_xlsx)
        rules = loader.load_rules()
        
        assert len(rules) == 2
        assert rules[0].regra_id == 1
        assert rules[1].regra_id == 2
    
    def test_find_matching_rule(self, temp_triggers_xlsx):
        """Teste: Encontrar regra correspondente"""
        loader = TriggerLoader(temp_triggers_xlsx)
        loader.load_rules()
        
        record = PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            status_bilhete=PortabilidadeStatus.CONCLUIDA,
            operadora_doadora="CLARO",
            ultimo_bilhete=True,
        )
        
        rule = loader.find_matching_rule(record)
        
        assert rule is not None
        assert rule.regra_id == 2
        assert rule.o_que_aconteceu == "PORTADO"
    
    def test_find_matching_rule_sem_match(self, temp_triggers_xlsx):
        """Teste: Não encontrar regra correspondente"""
        loader = TriggerLoader(temp_triggers_xlsx)
        loader.load_rules()
        
        record = PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            status_bilhete=PortabilidadeStatus.SUSPENSA,  # Status não mapeado
            operadora_doadora="OPERADORA_NOVA",
            ultimo_bilhete=True,
        )
        
        rule = loader.find_matching_rule(record)
        
        assert rule is None
    
    def test_get_rule_by_id(self, temp_triggers_xlsx):
        """Teste: Buscar regra por ID"""
        loader = TriggerLoader(temp_triggers_xlsx)
        loader.load_rules()
        
        rule = loader.get_rule_by_id(1)
        
        assert rule is not None
        assert rule.regra_id == 1
        assert rule.status_bilhete == "Portabilidade Cancelada"
    
    def test_get_rules_stats(self, temp_triggers_xlsx):
        """Teste: Estatísticas das regras"""
        loader = TriggerLoader(temp_triggers_xlsx)
        loader.load_rules()
        
        stats = loader.get_rules_stats()
        
        assert stats['total_regras'] == 2
        assert 'por_tipo_mensagem' in stats
        assert 'por_acao' in stats


class TestTriggerRule:
    """Testes para o modelo TriggerRule"""
    
    def test_from_dict(self):
        """Teste: Criar TriggerRule a partir de dicionário"""
        data = {
            'REGRA_ID': 1,
            'Status do bilhete': 'Portado',
            'Operadora doadora': 'VIVO',
            'Último bilhete de portabilidade?': 'Sim',
            'O que aconteceu': 'BP FECHADO',
            'Ação a ser realizada': 'PARABENIZAÇÃO',
            'Tipo de mensagem': 'CONFIRMACAO',
            'Templete': '1',
        }
        
        rule = TriggerRule.from_dict(data)
        
        assert rule.regra_id == 1
        assert rule.status_bilhete == 'Portado'
        assert rule.operadora_doadora == 'VIVO'
        assert rule.ultimo_bilhete is True
        assert rule.o_que_aconteceu == 'BP FECHADO'
    
    def test_to_dict(self):
        """Teste: Converter TriggerRule para dicionário"""
        rule = TriggerRule(
            regra_id=1,
            status_bilhete='Portado',
            operadora_doadora='VIVO',
            o_que_aconteceu='BP FECHADO',
        )
        
        data = rule.to_dict()
        
        assert data['regra_id'] == 1
        assert data['status_bilhete'] == 'Portado'
        assert data['operadora_doadora'] == 'VIVO'
        assert data['o_que_aconteceu'] == 'BP FECHADO'
    
    def test_clean_nan_values(self):
        """Teste: Limpar valores NaN"""
        import math
        
        data = {
            'REGRA_ID': 1,
            'Status do bilhete': float('nan'),  # NaN value
            'Operadora doadora': 'VIVO',
            'O que aconteceu': 'TESTE',
        }
        
        rule = TriggerRule.from_dict(data)
        
        assert rule.status_bilhete is None  # NaN deve ser convertido para None
        assert rule.operadora_doadora == 'VIVO'
