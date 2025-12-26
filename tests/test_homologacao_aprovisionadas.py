"""
Testes de Homologação para Aprovisionadas com Confirmação de Entrega
Testa geração de arquivo de aprovisionamentos, filtros e validações
"""
import pytest
import tempfile
import os
import csv
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem
from src.utils.csv_generator import CSVGenerator
from src.engine.qigger_decision_engine import DecisionResult


class TestHomologacaoAprovisionadas:
    """Testes para homologação de aprovisionadas com confirmação de entrega"""
    
    @pytest.fixture
    def record_em_aprovisionamento(self):
        """Fixture: Registro em aprovisionamento"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.EM_APROVISIONAMENTO,
            status_ordem=StatusOrdem.EM_APROVISIONAMENTO,
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 1),
            preco_ordem="99.90",
            cod_rastreio="https://tim.trakin.co/o/26-025001234"
        )
    
    @pytest.fixture
    def record_erro_aprovisionamento(self):
        """Fixture: Registro com erro de aprovisionamento"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001235",
            status_bilhete=PortabilidadeStatus.ERRO_APROVISIONAMENTO,
            status_ordem=StatusOrdem.ERRO_APROVISIONAMENTO,
            operadora_doadora="CLARO",
            data_portabilidade=datetime(2025, 12, 1),
            preco_ordem="99.90"
        )
    
    @pytest.fixture
    def record_com_confirmacao_entrega(self):
        """Fixture: Registro com confirmação de entrega"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001236",
            status_bilhete=PortabilidadeStatus.EM_APROVISIONAMENTO,
            status_ordem=StatusOrdem.EM_APROVISIONAMENTO,
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 1),
            preco_ordem="99.90",
            cod_rastreio="https://tim.trakin.co/o/26-025001236",
            nome_cliente="João Silva",
            telefone_contato="11987654321",
            cidade="São Paulo",
            uf="SP",
            cep="01234567"
        )
        # Simular confirmação de entrega via status_logistica
        record.status_logistica = "ENTREGUE"
        return record
    
    @pytest.fixture
    def record_nao_aprovisionado(self):
        """Fixture: Registro que não é aprovisionado"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001237",
            status_bilhete=PortabilidadeStatus.CONCLUIDA,
            status_ordem=StatusOrdem.CONCLUIDO,
            operadora_doadora="VIVO"
        )
    
    @pytest.fixture
    def results_map_aprovisionamento(self, record_em_aprovisionamento):
        """Fixture: Mapa de resultados para aprovisionamento"""
        result = DecisionResult(
            rule_name="trigger_rule_21",
            decision="APROVISIONAR",
            action="APROVISIONAR",
            details="Registro em aprovisionamento",
            priority=2,
            regra_id=21,
            o_que_aconteceu="EM APROVISIONAMENTO",
            acao_a_realizar="APROVISIONAR"
        )
        key = f"{record_em_aprovisionamento.cpf}_{record_em_aprovisionamento.numero_ordem}"
        return {key: [result]}
    
    @pytest.fixture
    def results_map_erro_aprovisionamento(self, record_erro_aprovisionamento):
        """Fixture: Mapa de resultados para erro de aprovisionamento"""
        result = DecisionResult(
            rule_name="rule_10_erro_aprovisionamento",
            decision="CORRIGIR_APROVISIONAMENTO",
            action="REPROCESSAR",
            details="Erro no aprovisionamento",
            priority=1,
            regra_id=10,
            o_que_aconteceu="ERRO APROVISIONAMENTO",
            acao_a_realizar="REPROCESSAR"
        )
        key = f"{record_erro_aprovisionamento.cpf}_{record_erro_aprovisionamento.numero_ordem}"
        return {key: [result]}
    
    # ========== TESTES DE FILTRO DE APROVISIONADOS ==========
    
    def test_filtrar_aprovisionados_por_status_bilhete(self, record_em_aprovisionamento):
        """Teste: Filtrar aprovisionados por status do bilhete"""
        assert record_em_aprovisionamento.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO
        
        # Verificar se seria incluído no CSV
        is_aprovisionado = (
            record_em_aprovisionamento.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO or
            record_em_aprovisionamento.status_ordem == StatusOrdem.EM_APROVISIONAMENTO
        )
        
        assert is_aprovisionado is True
    
    def test_filtrar_aprovisionados_por_status_ordem(self, record_em_aprovisionamento):
        """Teste: Filtrar aprovisionados por status da ordem"""
        assert record_em_aprovisionamento.status_ordem == StatusOrdem.EM_APROVISIONAMENTO
        
        is_aprovisionado = (
            record_em_aprovisionamento.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO or
            record_em_aprovisionamento.status_ordem == StatusOrdem.EM_APROVISIONAMENTO
        )
        
        assert is_aprovisionado is True
    
    def test_filtrar_aprovisionados_por_resultado_decisao(self, record_em_aprovisionamento, results_map_aprovisionamento):
        """Teste: Filtrar aprovisionados por resultado de decisão"""
        key = f"{record_em_aprovisionamento.cpf}_{record_em_aprovisionamento.numero_ordem}"
        results = results_map_aprovisionamento.get(key, [])
        
        is_aprovisionado = False
        for result in results:
            if result.decision in ['APROVISIONAR', 'CORRIGIR_APROVISIONAMENTO', 'REPROCESSAR']:
                is_aprovisionado = True
                break
            if 'rule_10_erro_aprovisionamento' in result.rule_name:
                is_aprovisionado = True
                break
            if 'rule_21_em_aprovisionamento' in result.rule_name:
                is_aprovisionado = True
                break
        
        assert is_aprovisionado is True
    
    def test_nao_filtrar_nao_aprovisionados(self, record_nao_aprovisionado):
        """Teste: Não filtrar registros que não são aprovisionados"""
        is_aprovisionado = (
            record_nao_aprovisionado.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO or
            record_nao_aprovisionado.status_ordem == StatusOrdem.EM_APROVISIONAMENTO
        )
        
        assert is_aprovisionado is False
    
    # ========== TESTES DE GERAÇÃO DE CSV ==========
    
    def test_gerar_csv_aprovisionamentos(self, record_em_aprovisionamento, results_map_aprovisionamento):
        """Teste: Gerar CSV de aprovisionamentos"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_em_aprovisionamento]
            result = CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map_aprovisionamento,
                Path(temp_path)
            )
            
            assert result is True
            assert os.path.exists(temp_path)
            
            # Verificar conteúdo
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) == 1
                row = rows[0]
                assert row['CPF'] == "52998224725"
                assert row['Codigo_Externo'] == "250001234"
                assert row['Status_Bilhete'] == "Em Aprovisionamento"
                assert row['Status_Ordem'] == "Em Aprovisionamento"
                assert 'Cod_Rastreio' in row
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_gerar_csv_aprovisionamentos_vazio(self, record_nao_aprovisionado):
        """Teste: Não gerar CSV se não houver aprovisionados"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_nao_aprovisionado]
            results_map = {}
            
            result = CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map,
                Path(temp_path)
            )
            
            assert result is False
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_gerar_csv_aprovisionamentos_com_erro(self, record_erro_aprovisionamento, results_map_erro_aprovisionamento):
        """Teste: Gerar CSV com registros de erro de aprovisionamento"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_erro_aprovisionamento]
            result = CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map_erro_aprovisionamento,
                Path(temp_path)
            )
            
            assert result is True
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) == 1
                row = rows[0]
                assert row['Status_Bilhete'] == "Erro no Aprovisionamento"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    # ========== TESTES DE CONFIRMAÇÃO DE ENTREGA ==========
    
    def test_validar_confirmacao_entrega_por_status_logistica(self, record_com_confirmacao_entrega):
        """Teste: Validar confirmação de entrega por status de logística"""
        assert record_com_confirmacao_entrega.status_logistica == "ENTREGUE"
        
        # Verificar se tem dados de entrega
        tem_dados_entrega = (
            record_com_confirmacao_entrega.cod_rastreio is not None and
            record_com_confirmacao_entrega.nome_cliente is not None and
            record_com_confirmacao_entrega.cidade is not None
        )
        
        assert tem_dados_entrega is True
    
    def test_validar_link_rastreio_em_aprovisionados(self, record_em_aprovisionamento):
        """Teste: Validar que link de rastreio está presente"""
        assert record_em_aprovisionamento.cod_rastreio is not None
        assert record_em_aprovisionamento.cod_rastreio.startswith("https://tim.trakin.co/o/")
    
    def test_gerar_link_rastreio_se_ausente(self, record_em_aprovisionamento):
        """Teste: Gerar link de rastreio se ausente"""
        record_em_aprovisionamento.cod_rastreio = None
        
        # Simular geração de link
        if not record_em_aprovisionamento.cod_rastreio:
            link = PortabilidadeRecord.gerar_link_rastreio(record_em_aprovisionamento.codigo_externo)
            record_em_aprovisionamento.cod_rastreio = link
        
        assert record_em_aprovisionamento.cod_rastreio is not None
        # O método gerar_link_rastreio apenas adiciona o código ao link, sem formatar com prefixo 26-
        assert "250001234" in record_em_aprovisionamento.cod_rastreio or "26-025001234" in record_em_aprovisionamento.cod_rastreio
    
    def test_validar_dados_completos_para_entrega(self, record_com_confirmacao_entrega):
        """Teste: Validar que todos os dados necessários para entrega estão presentes"""
        dados_completos = (
            record_com_confirmacao_entrega.cpf is not None and
            record_com_confirmacao_entrega.nome_cliente is not None and
            record_com_confirmacao_entrega.telefone_contato is not None and
            record_com_confirmacao_entrega.cidade is not None and
            record_com_confirmacao_entrega.uf is not None and
            record_com_confirmacao_entrega.cep is not None and
            record_com_confirmacao_entrega.cod_rastreio is not None
        )
        
        assert dados_completos is True
    
    # ========== TESTES DE CAMPOS DO CSV ==========
    
    def test_validar_cabecalho_csv_aprovisionamentos(self, record_em_aprovisionamento, results_map_aprovisionamento):
        """Teste: Validar cabeçalho do CSV de aprovisionamentos"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_em_aprovisionamento]
            CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map_aprovisionamento,
                Path(temp_path)
            )
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                fieldnames = reader.fieldnames
                
                campos_esperados = [
                    'CPF', 'Numero_Acesso', 'Numero_Ordem', 'Codigo_Externo',
                    'Cod_Rastreio', 'Status_Bilhete', 'Status_Ordem',
                    'Operadora_Doadora', 'Data_Portabilidade', 'Preco_Ordem',
                    'Motivo_Recusa', 'Motivo_Cancelamento',
                    'Decisoes_Aplicadas', 'Acoes_Recomendadas'
                ]
                
                for campo in campos_esperados:
                    assert campo in fieldnames
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_validar_cod_rastreio_no_csv(self, record_em_aprovisionamento, results_map_aprovisionamento):
        """Teste: Validar que Cod_Rastreio está no CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_em_aprovisionamento]
            CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map_aprovisionamento,
                Path(temp_path)
            )
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) > 0
                row = rows[0]
                assert 'Cod_Rastreio' in row
                assert row['Cod_Rastreio'].startswith("https://tim.trakin.co/o/")
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    # ========== TESTES DE MÚLTIPLOS REGISTROS ==========
    
    def test_gerar_csv_multiplos_aprovisionados(self, record_em_aprovisionamento, record_erro_aprovisionamento, results_map_aprovisionamento, results_map_erro_aprovisionamento):
        """Teste: Gerar CSV com múltiplos registros aprovisionados"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_em_aprovisionamento, record_erro_aprovisionamento]
            results_map = {**results_map_aprovisionamento, **results_map_erro_aprovisionamento}
            
            result = CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map,
                Path(temp_path)
            )
            
            assert result is True
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) == 2
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_filtrar_apenas_aprovisionados_em_lote(self, record_em_aprovisionamento, record_nao_aprovisionado, results_map_aprovisionamento):
        """Teste: Filtrar apenas aprovisionados em lote misto"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_em_aprovisionamento, record_nao_aprovisionado]
            # Criar results_map vazio para o registro não aprovisionado
            key_nao_aprovisionado = f"{record_nao_aprovisionado.cpf}_{record_nao_aprovisionado.numero_ordem}"
            results_map = {**results_map_aprovisionamento}
            # Garantir que o registro não aprovisionado não tenha resultados que indiquem aprovisionamento
            results_map[key_nao_aprovisionado] = []
            
            result = CSVGenerator.generate_aprovisionamentos_csv(
                records,
                results_map,
                Path(temp_path)
            )
            
            assert result is True
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                # Deve ter apenas o aprovisionado
                assert len(rows) == 1
                assert rows[0]['Codigo_Externo'] == "250001234"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

