"""
Testes de Homologação para Vendas Canceladas e Reabertura de Orders
Testa geração de arquivo de reabertura, filtros de cancelamento e novo status de order
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


class TestHomologacaoReabertura:
    """Testes para homologação de vendas canceladas e reabertura de orders"""
    
    @pytest.fixture
    def record_cancelado(self):
        """Fixture: Registro com status cancelado"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.CANCELADA,
            status_ordem=StatusOrdem.PENDENTE,
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 1),
            motivo_cancelamento="Rejeição do Cliente via SMS",
            preco_ordem="99.90",
            cod_rastreio="https://tim.trakin.co/o/26-025001234"
        )
    
    @pytest.fixture
    def record_cancelamento_pendente(self):
        """Fixture: Registro com cancelamento pendente"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890124",
            codigo_externo="250001235",
            status_bilhete=PortabilidadeStatus.CANCELAMENTO_PENDENTE,
            status_ordem=StatusOrdem.PENDENTE,
            operadora_doadora="CLARO",
            data_portabilidade=datetime(2025, 12, 1),
            motivo_cancelamento="Cancelamento pendente",
            preco_ordem="99.90"
        )
    
    @pytest.fixture
    def record_com_motivo_cancelamento(self):
        """Fixture: Registro com motivo de cancelamento"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890125",
            codigo_externo="250001236",
            status_bilhete=PortabilidadeStatus.PENDENTE,
            status_ordem=StatusOrdem.PENDENTE,
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 1),
            motivo_cancelamento="Cancelado automaticamente pela BDR",
            preco_ordem="99.90"
        )
    
    @pytest.fixture
    def record_nao_cancelado(self):
        """Fixture: Registro que não está cancelado"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890126",
            codigo_externo="250001237",
            status_bilhete=PortabilidadeStatus.CONCLUIDA,
            status_ordem=StatusOrdem.CONCLUIDO,
            operadora_doadora="VIVO",
            motivo_cancelamento=None
        )
    
    @pytest.fixture
    def record_novo_status_order(self):
        """Fixture: Registro com novo status de order para reabertura"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890127",
            codigo_externo="250001238",
            status_bilhete=PortabilidadeStatus.CANCELADA,
            status_ordem=StatusOrdem.PENDENTE,  # Novo status para reabertura
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 1),
            motivo_cancelamento="Rejeição do Cliente via SMS",
            preco_ordem="99.90",
            novo_status_bilhete="Pendente Portabilidade",  # Novo status
            cod_rastreio="https://tim.trakin.co/o/26-025001238"
        )
        return record
    
    @pytest.fixture
    def results_map_cancelado(self, record_cancelado):
        """Fixture: Mapa de resultados para cancelado"""
        result = DecisionResult(
            rule_name="rule_05_portabilidade_cancelada",
            decision="CANCELAR",
            action="REABRIR",
            details="Portabilidade cancelada - reabertura necessária",
            priority=1,
            regra_id=5,
            o_que_aconteceu="CANCELAMENTO AUTOMATICO",
            acao_a_realizar="REABERTURA"
        )
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        return {key: [result]}
    
    @pytest.fixture
    def results_map_reabertura(self, record_cancelado):
        """Fixture: Mapa de resultados para reabertura"""
        result = DecisionResult(
            rule_name="trigger_rule_14",
            decision="REABRIR",
            action="REABRIR",
            details="Reabertura de order cancelada",
            priority=1,
            regra_id=14,
            o_que_aconteceu="CANCELAMENTO AUTOMATICO",
            acao_a_realizar="REABERTURA"
        )
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        return {key: [result]}
    
    @pytest.fixture
    def results_map_reagendar(self, record_cancelado):
        """Fixture: Mapa de resultados para reagendar"""
        result = DecisionResult(
            rule_name="trigger_rule_15",
            decision="REAGENDAR",
            action="REAGENDAR",
            details="Reagendar portabilidade",
            priority=2,
            regra_id=15,
            o_que_aconteceu="CANCELAMENTO PENDENTE",
            acao_a_realizar="REAGENDAR"
        )
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        return {key: [result]}
    
    # ========== TESTES DE FILTRO DE CANCELADOS ==========
    
    def test_filtrar_cancelados_por_status_bilhete(self, record_cancelado):
        """Teste: Filtrar cancelados por status do bilhete"""
        assert record_cancelado.status_bilhete == PortabilidadeStatus.CANCELADA
        
        is_reabertura = (
            record_cancelado.status_bilhete == PortabilidadeStatus.CANCELADA
        )
        
        assert is_reabertura is True
    
    def test_filtrar_cancelados_por_motivo_cancelamento(self, record_com_motivo_cancelamento):
        """Teste: Filtrar cancelados por motivo de cancelamento"""
        assert record_com_motivo_cancelamento.motivo_cancelamento is not None
        
        is_reabertura = False
        if record_com_motivo_cancelamento.motivo_cancelamento:
            if any(termo in record_com_motivo_cancelamento.motivo_cancelamento.lower() 
                   for termo in ['cancelamento', 'cancelado', 'pendente']):
                is_reabertura = True
        
        assert is_reabertura is True
    
    def test_filtrar_cancelados_por_resultado_decisao(self, record_cancelado, results_map_cancelado):
        """Teste: Filtrar cancelados por resultado de decisão"""
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        results = results_map_cancelado.get(key, [])
        
        is_reabertura = False
        for result in results:
            if result.decision in ['CANCELAR', 'REABRIR', 'REAGENDAR']:
                is_reabertura = True
                break
            if 'rule_05_portabilidade_cancelada' in result.rule_name:
                is_reabertura = True
                break
            if 'rule_14_motivo_cancelamento' in result.rule_name:
                is_reabertura = True
                break
        
        assert is_reabertura is True
    
    def test_nao_filtrar_nao_cancelados(self, record_nao_cancelado):
        """Teste: Não filtrar registros que não estão cancelados"""
        is_reabertura = (
            record_nao_cancelado.status_bilhete == PortabilidadeStatus.CANCELADA
        )
        
        if not is_reabertura and record_nao_cancelado.motivo_cancelamento:
            is_reabertura = any(termo in record_nao_cancelado.motivo_cancelamento.lower() 
                               for termo in ['cancelamento', 'cancelado', 'pendente'])
        
        assert is_reabertura is False
    
    # ========== TESTES DE NOVO STATUS DE ORDER ==========
    
    def test_validar_novo_status_bilhete(self, record_novo_status_order):
        """Teste: Validar novo status de bilhete para reabertura"""
        assert record_novo_status_order.novo_status_bilhete is not None
        assert record_novo_status_order.novo_status_bilhete == "Pendente Portabilidade"
        
        # Verificar que status original é cancelado
        assert record_novo_status_order.status_bilhete == PortabilidadeStatus.CANCELADA
    
    def test_validar_novo_status_ordem(self, record_novo_status_order):
        """Teste: Validar novo status de ordem para reabertura"""
        assert record_novo_status_order.status_ordem == StatusOrdem.PENDENTE
        
        # Status PENDENTE indica que order pode ser reaberta
        pode_reabrir = (
            record_novo_status_order.status_ordem == StatusOrdem.PENDENTE and
            record_novo_status_order.status_bilhete == PortabilidadeStatus.CANCELADA
        )
        
        assert pode_reabrir is True
    
    def test_validar_transicao_status_cancelado_para_pendente(self, record_novo_status_order):
        """Teste: Validar transição de status cancelado para pendente"""
        # Status original
        status_original = record_novo_status_order.status_bilhete
        assert status_original == PortabilidadeStatus.CANCELADA
        
        # Novo status
        novo_status = record_novo_status_order.novo_status_bilhete
        assert novo_status == "Pendente Portabilidade"
        
        # Verificar que há transição válida
        transicao_valida = (
            status_original == PortabilidadeStatus.CANCELADA and
            novo_status is not None and
            novo_status != "Portabilidade Cancelada"
        )
        
        assert transicao_valida is True
    
    # ========== TESTES DE GERAÇÃO DE CSV ==========
    
    def test_gerar_csv_reabertura(self, record_cancelado, results_map_reabertura):
        """Teste: Gerar CSV de reabertura"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado]
            result = CSVGenerator.generate_reabertura_csv(
                records,
                results_map_reabertura,
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
                assert row['Status_Bilhete'] == "Portabilidade Cancelada"
                assert row['Motivo_Cancelamento'] == "Rejeição do Cliente via SMS"
                assert 'Cod_Rastreio' in row
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_gerar_csv_reabertura_vazio(self, record_nao_cancelado):
        """Teste: Não gerar CSV se não houver cancelados"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_nao_cancelado]
            results_map = {}
            
            result = CSVGenerator.generate_reabertura_csv(
                records,
                results_map,
                Path(temp_path)
            )
            
            assert result is False
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_gerar_csv_reabertura_com_novo_status(self, record_novo_status_order, results_map_reabertura):
        """Teste: Gerar CSV com novo status de order"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_novo_status_order]
            result = CSVGenerator.generate_reabertura_csv(
                records,
                results_map_reabertura,
                Path(temp_path)
            )
            
            assert result is True
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) == 1
                row = rows[0]
                assert row['Status_Bilhete'] == "Portabilidade Cancelada"
                assert row['Status_Ordem'] == "Pendente Portabilidade"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    # ========== TESTES DE CAMPOS DO CSV ==========
    
    def test_validar_cabecalho_csv_reabertura(self, record_cancelado, results_map_reabertura):
        """Teste: Validar cabeçalho do CSV de reabertura"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado]
            CSVGenerator.generate_reabertura_csv(
                records,
                results_map_reabertura,
                Path(temp_path)
            )
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                fieldnames = reader.fieldnames
                
                campos_esperados = [
                    'CPF', 'Numero_Acesso', 'Numero_Ordem', 'Codigo_Externo',
                    'Cod_Rastreio', 'Status_Bilhete', 'Status_Ordem',
                    'Operadora_Doadora', 'Data_Portabilidade',
                    'Motivo_Cancelamento', 'Motivo_Recusa', 'Preco_Ordem',
                    'Decisoes_Aplicadas', 'Acoes_Recomendadas'
                ]
                
                for campo in campos_esperados:
                    assert campo in fieldnames
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_validar_cod_rastreio_no_csv_reabertura(self, record_cancelado, results_map_reabertura):
        """Teste: Validar que Cod_Rastreio está no CSV de reabertura"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado]
            CSVGenerator.generate_reabertura_csv(
                records,
                results_map_reabertura,
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
    
    def test_gerar_csv_multiplos_cancelados(self, record_cancelado, record_cancelamento_pendente, results_map_reabertura):
        """Teste: Gerar CSV com múltiplos registros cancelados"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado, record_cancelamento_pendente]
            
            # Criar results_map para ambos
            key1 = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
            key2 = f"{record_cancelamento_pendente.cpf}_{record_cancelamento_pendente.numero_ordem}"
            results_map = {
                key1: results_map_reabertura[key1],
                key2: results_map_reabertura[key1]  # Reutilizar mesmo resultado
            }
            
            result = CSVGenerator.generate_reabertura_csv(
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
    
    def test_filtrar_apenas_cancelados_em_lote(self, record_cancelado, record_nao_cancelado, results_map_reabertura):
        """Teste: Filtrar apenas cancelados em lote misto"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado, record_nao_cancelado]
            results_map = results_map_reabertura
            
            result = CSVGenerator.generate_reabertura_csv(
                records,
                results_map,
                Path(temp_path)
            )
            
            assert result is True
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                # Deve ter apenas o cancelado
                assert len(rows) == 1
                assert rows[0]['Codigo_Externo'] == "250001234"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    # ========== TESTES DE AÇÕES DE REABERTURA ==========
    
    def test_validar_acao_reabertura(self, record_cancelado, results_map_reabertura):
        """Teste: Validar ação de reabertura"""
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        results = results_map_reabertura.get(key, [])
        
        acao_reabertura = False
        for result in results:
            if result.action == "REABRIR" or result.acao_a_realizar == "REABERTURA":
                acao_reabertura = True
                break
        
        assert acao_reabertura is True
    
    def test_validar_acao_reagendar(self, record_cancelado, results_map_reagendar):
        """Teste: Validar ação de reagendar"""
        key = f"{record_cancelado.cpf}_{record_cancelado.numero_ordem}"
        results = results_map_reagendar.get(key, [])
        
        acao_reagendar = False
        for result in results:
            if result.action == "REAGENDAR" or result.acao_a_realizar == "REAGENDAR":
                acao_reagendar = True
                break
        
        assert acao_reagendar is True
    
    def test_validar_decisoes_aplicadas_no_csv(self, record_cancelado, results_map_reabertura):
        """Teste: Validar que decisões aplicadas estão no CSV"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            records = [record_cancelado]
            CSVGenerator.generate_reabertura_csv(
                records,
                results_map_reabertura,
                Path(temp_path)
            )
            
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                rows = list(reader)
                
                assert len(rows) > 0
                row = rows[0]
                assert 'Decisoes_Aplicadas' in row
                assert 'Acoes_Recomendadas' in row
                assert row['Decisoes_Aplicadas'] != ""
                assert row['Acoes_Recomendadas'] != ""
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

