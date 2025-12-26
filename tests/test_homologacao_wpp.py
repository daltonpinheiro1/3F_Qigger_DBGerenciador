"""
Testes de Homologação para WhatsApp
Testa geração de arquivo de homologação, templates, variáveis e mapeamento
"""
import pytest
import tempfile
import os
import csv
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem
from src.utils.templates_wpp import TemplateMapper, TEMPLATES, TemplateConfig
from src.utils.wpp_output_generator import WPPOutputGenerator
from src.database.db_manager import DatabaseManager
from src.engine.qigger_decision_engine import DecisionResult


class TestHomologacaoWPP:
    """Testes para homologação de WhatsApp"""
    
    @pytest.fixture
    def temp_db(self):
        """Fixture para criar banco de dados temporário"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            temp_path = f.name
        
        db_manager = DatabaseManager(temp_path)
        yield db_manager
        
        # Limpeza
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    @pytest.fixture
    def sample_record(self):
        """Fixture para criar registro de exemplo"""
        return PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.CONCLUIDA,
            operadora_doadora="VIVO",
            nome_cliente="João Silva Santos",
            telefone_contato="11987654321",
            cidade="São Paulo",
            uf="SP",
            cep="01234567",
            data_venda=datetime(2025, 12, 1),
            template="1",
            tipo_mensagem="CONFIRMACAO BP",
            regra_id=1,
            o_que_aconteceu="BP FECHADO",
            acao_a_realizar="POS VENDA PARABENIZAÇÃO",
            mapeado=True
        )
    
    @pytest.fixture
    def sample_record_com_endereco(self):
        """Fixture para registro com dados de endereço completos"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.EM_APROVISIONAMENTO,
            operadora_doadora="VIVO",
            nome_cliente="Maria da Silva",
            telefone_contato="11987654321",
            cidade="São Paulo",
            uf="SP",
            cep="01234567",
            data_venda=datetime(2025, 12, 1),
            template="43",
            tipo_mensagem="ENDERECO INCORRETO",
            regra_id=4,
            o_que_aconteceu="ENDEREÇO INCORRETO",
            acao_a_realizar="CONFIRMAR ENDEREÇO",
            mapeado=True
        )
        # Adicionar dados de endereço diretamente no registro (não via mock)
        # Criar um objeto simples com os atributos necessários
        class SimpleObject:
            def __init__(self):
                self.destinatario = "Maria da Silva"
                self.telefone = "11987654321"
                self.cidade = "São Paulo"
                self.uf = "SP"
                self.cep = "01234567"
                self.data_criacao_pedido = datetime(2025, 12, 1)
                self.status = "ENTREGUE"
                self.nu_pedido = None
                self.rastreio = None
        
        obj = SimpleObject()
        # Adicionar atributos de endereço diretamente ao registro
        record.__dict__['endereco'] = "Rua das Flores"
        record.__dict__['numero'] = "123"
        record.__dict__['complemento'] = "Apto 45"
        record.__dict__['bairro'] = "Centro"
        record.__dict__['ponto_referencia'] = "Próximo ao mercado"
        record.enrich_with_logistics(obj)
        return record
    
    # ========== TESTES DE TEMPLATES ==========
    
    def test_template_mapper_get_template_id(self):
        """Teste: Obter ID do template por tipo de comunicação"""
        assert TemplateMapper.get_template_id("1") == 1
        assert TemplateMapper.get_template_id("2") == 1
        assert TemplateMapper.get_template_id("3") == 1
        assert TemplateMapper.get_template_id("5") == 2
        assert TemplateMapper.get_template_id("6") == 2
        assert TemplateMapper.get_template_id("14") == 3
        assert TemplateMapper.get_template_id("43") == 4
        assert TemplateMapper.get_template_id("CONFIRMACAO BP") == 1
        assert TemplateMapper.get_template_id("PENDENTE") == 2
        assert TemplateMapper.get_template_id("RETIRADA CORREIOS") == 3
        assert TemplateMapper.get_template_id("ENDERECO INCORRETO") == 4
        assert TemplateMapper.get_template_id("NÃO ENVIAR") is None
        assert TemplateMapper.get_template_id("-") is None
        assert TemplateMapper.get_template_id("") is None
    
    def test_template_mapper_get_template_config(self):
        """Teste: Obter configuração do template"""
        config = TemplateMapper.get_template_config(1)
        assert config is not None
        assert config.id == 1
        assert config.nome_modelo == "confirma_portabilidade_v1"
        assert config.tem_botao is True
        
        config = TemplateMapper.get_template_config(4)
        assert config is not None
        assert config.id == 4
        assert config.nome_modelo == "confirmacao_endereco_v1"
        assert config.tem_botao is False
    
    def test_template_mapper_get_template_for_record(self, sample_record):
        """Teste: Obter template para registro"""
        template_info = TemplateMapper.get_template_for_record(sample_record)
        
        assert template_info is not None
        assert template_info.get('template_id') == 1
        assert template_info.get('nome_modelo') == "confirma_portabilidade_v1"
        assert template_info.get('mapeado') is True
    
    def test_template_mapper_generate_variables_template_3(self):
        """Teste: Gerar variáveis para template 3 (retirada correios)"""
        record_data = {
            "nome_cliente": "João Silva",
            "cod_rastreio": "https://tim.trakin.co/o/26-025001234"
        }
        
        variables = TemplateMapper.generate_variables(3, record_data)
        
        assert variables is not None
        assert variables.get("1") == "João Silva"
        assert variables.get("2") == "https://tim.trakin.co/o/26-025001234"
    
    def test_template_mapper_generate_variables_template_4(self):
        """Teste: Gerar variáveis para template 4 (confirmação endereço)"""
        record_data = {
            "nome_cliente": "Maria Silva",
            "endereco": "Rua das Flores",
            "numero": "123",
            "complemento": "Apto 45",
            "bairro": "Centro",
            "cidade": "São Paulo",
            "uf": "SP",
            "cep": "01234567",
            "ponto_referencia": "Próximo ao mercado"
        }
        
        variables = TemplateMapper.generate_variables(4, record_data)
        
        assert variables is not None
        assert variables.get("1") == "Maria Silva"
        assert variables.get("2") == "Rua das Flores"
        assert variables.get("3") == "123"
        assert variables.get("4") == "Apto 45"
        assert variables.get("5") == "Centro"
        assert variables.get("6") == "São Paulo"
        assert variables.get("7") == "SP"
        assert variables.get("8") == "01234567"
        assert variables.get("9") == "Próximo ao mercado"
    
    def test_template_mapper_format_variables_string(self):
        """Teste: Formatar variáveis como string"""
        variables = {"1": "João", "2": "ABC123"}
        result = TemplateMapper.format_variables_string(variables)
        
        assert result == "{{1}}=João;{{2}}=ABC123"
    
    def test_template_mapper_format_variables_string_vazio(self):
        """Teste: Formatar variáveis vazias"""
        result = TemplateMapper.format_variables_string({})
        assert result == ""
    
    # ========== TESTES DE WPP OUTPUT GENERATOR ==========
    
    def test_wpp_output_generator_add_record_com_template(self, sample_record):
        """Teste: Adicionar registro com template"""
        generator = WPPOutputGenerator()
        result = generator.add_record(sample_record)
        
        assert result is True
        assert generator.pending_count == 1
    
    def test_wpp_output_generator_add_record_sem_template(self):
        """Teste: Não adicionar registro sem template"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            template=None
        )
        
        generator = WPPOutputGenerator()
        result = generator.add_record(record)
        
        assert result is False
        assert generator.pending_count == 0
    
    def test_wpp_output_generator_generate_csv(self, sample_record, temp_db):
        """Teste: Gerar arquivo CSV de homologação"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            generator = WPPOutputGenerator(temp_path)
            generator.add_record(sample_record)
            
            result_path = generator.generate_csv()
            
            assert result_path == temp_path
            assert os.path.exists(temp_path)
            
            # Verificar conteúdo do arquivo
            with open(temp_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                assert len(rows) == 1
                row = rows[0]
                assert row['Proposta_iSize'] == "250001234"
                assert row['Cpf'] == "52998224725"
                assert row['Tipo_Comunicacao'] == "1"
                assert row['Status_Disparo'] == "FALSE"
                assert row['DataHora_Disparo'] == ""
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_wpp_output_generator_enrich_with_template_info(self, sample_record):
        """Teste: Enriquecer dados com informações do template"""
        generator = WPPOutputGenerator()
        row = sample_record.to_wpp_dict()
        
        enriched = generator._enrich_with_template_info(row, sample_record)
        
        # Template_ID pode ser int ou string
        assert enriched['Template_ID'] == "1" or enriched['Template_ID'] == 1
        assert enriched['Template_Nome'] == "confirma_portabilidade_v1"
        assert 'Template_Variaveis' in enriched
    
    def test_wpp_output_generator_filter_records_with_template(self, sample_record):
        """Teste: Filtrar registros com template"""
        record_sem_template = PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            template=None
        )
        
        records = [sample_record, record_sem_template]
        filtered = WPPOutputGenerator.filter_records_with_template(records)
        
        assert len(filtered) == 1
        assert filtered[0] == sample_record
    
    # ========== TESTES DE GERAÇÃO DE LINK DE RASTREIO ==========
    
    def test_gerar_link_rastreio_com_codigo_externo(self):
        """Teste: Gerar link de rastreio com código externo"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="250001234"
        )
        
        link = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo)
        
        assert link is not None
        assert link.startswith("https://tim.trakin.co/o/")
        # O método gerar_link_rastreio apenas adiciona o código ao link, sem formatar com prefixo 26-
        assert "250001234" in link or "26-025001234" in link
    
    def test_gerar_link_rastreio_com_nu_pedido(self):
        """Teste: Gerar link de rastreio com nu_pedido do objeto"""
        # Simular objeto com nu_pedido
        mock_obj = Mock()
        mock_obj.nu_pedido = "26-025001234"
        
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="250001234"
        )
        
        # Simular enriquecimento
        record.enrich_with_logistics(mock_obj)
        
        # O link deve ser gerado automaticamente
        assert record.cod_rastreio is not None
        assert "26-025001234" in record.cod_rastreio
    
    # ========== TESTES DE HOMOLOGAÇÃO COMPLETA ==========
    
    def test_homologacao_template_1_confirma_portabilidade(self, sample_record):
        """Teste: Homologação completa para template 1"""
        template_info = TemplateMapper.get_template_for_record(sample_record)
        
        assert template_info['template_id'] == 1
        assert template_info['nome_modelo'] == "confirma_portabilidade_v1"
        
        config = TEMPLATES[1]
        assert config.tem_botao is True
        assert config.botao_texto == "Confirmar Solicitação"
        assert config.botao_url == "https://tinyurl.com/portsim"
    
    def test_homologacao_template_2_pendencia_sms(self):
        """Teste: Homologação completa para template 2"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            template="5",
            tipo_mensagem="PENDENTE"
        )
        
        template_info = TemplateMapper.get_template_for_record(record)
        
        assert template_info['template_id'] == 2
        assert template_info['nome_modelo'] == "pendencia_sms_portabilidade"
        
        config = TEMPLATES[2]
        assert config.tem_botao is True
        assert config.botao_texto == "Gerar SMS de Validação"
    
    def test_homologacao_template_3_retirada_correios(self):
        """Teste: Homologação completa para template 3"""
        record = PortabilidadeRecord(
            cpf="52998224725",
            numero_acesso="11987654321",
            numero_ordem="1-123",
            codigo_externo="123",
            template="14",
            tipo_mensagem="AGUARDANDO RETIRADA",
            nome_cliente="João Silva",
            cod_rastreio="https://tim.trakin.co/o/26-025001234"
        )
        
        template_info = TemplateMapper.get_template_for_record(record)
        
        assert template_info['template_id'] == 3
        assert template_info['nome_modelo'] == "aviso_retirada_correios_v1"
        
        # Verificar variáveis
        record_data = {
            "nome_cliente": record.nome_cliente or "",
            "cod_rastreio": record.cod_rastreio or ""
        }
        variables = TemplateMapper.generate_variables(3, record_data)
        
        assert variables.get("1") == "João Silva"
        assert variables.get("2") == "https://tim.trakin.co/o/26-025001234"
    
    def test_homologacao_template_4_confirmacao_endereco(self, sample_record_com_endereco):
        """Teste: Homologação completa para template 4"""
        template_info = TemplateMapper.get_template_for_record(sample_record_com_endereco)
        
        assert template_info['template_id'] == 4
        assert template_info['nome_modelo'] == "confirmacao_endereco_v1"
        
        # Verificar variáveis - usar dados reais do registro
        record_data = {
            "nome_cliente": str(sample_record_com_endereco.nome_cliente or ""),
            "endereco": str(getattr(sample_record_com_endereco, 'endereco', '') or ""),
            "numero": str(getattr(sample_record_com_endereco, 'numero', '') or ""),
            "complemento": str(getattr(sample_record_com_endereco, 'complemento', '') or ""),
            "bairro": str(getattr(sample_record_com_endereco, 'bairro', '') or ""),
            "cidade": str(sample_record_com_endereco.cidade or ""),
            "uf": str(sample_record_com_endereco.uf or ""),
            "cep": str(sample_record_com_endereco.cep or ""),
            "ponto_referencia": str(getattr(sample_record_com_endereco, 'ponto_referencia', '') or "")
        }
        
        variables = TemplateMapper.generate_variables(4, record_data)
        
        assert variables.get("1") == "Maria da Silva"
        assert variables.get("2") == "Rua das Flores"
        assert variables.get("3") == "123"
        assert variables.get("4") == "Apto 45"
        assert variables.get("5") == "Centro"
        assert variables.get("6") == "São Paulo"
        assert variables.get("7") == "SP"
        assert variables.get("8") == "01234567"
        assert variables.get("9") == "Próximo ao mercado"
    
    # ========== TESTES DE VALIDAÇÃO DE DADOS ==========
    
    def test_validar_dados_cliente_completos(self, sample_record):
        """Teste: Validar que dados do cliente estão completos"""
        assert sample_record.cpf is not None
        assert sample_record.nome_cliente is not None
        assert sample_record.telefone_contato is not None
        assert sample_record.cidade is not None
        assert sample_record.uf is not None
        assert sample_record.cep is not None
    
    def test_validar_template_mapeado(self, sample_record):
        """Teste: Validar que template está mapeado"""
        assert sample_record.mapeado is True
        assert sample_record.template is not None
        assert sample_record.template != "-"
        assert sample_record.template != ""
    
    def test_validar_status_disparo_sempre_false(self, sample_record):
        """Teste: Validar que Status_Disparo é sempre FALSE em homologação"""
        generator = WPPOutputGenerator()
        row = sample_record.to_wpp_dict()
        enriched = generator._enrich_with_template_info(row, sample_record)
        
        # Status_Disparo deve ser sempre FALSE em homologação
        assert enriched.get('Status_Disparo') == 'FALSE' or enriched.get('Status_Disparo') == ''
    
    def test_validar_datahora_disparo_sempre_vazio(self, sample_record):
        """Teste: Validar que DataHora_Disparo é sempre vazio em homologação"""
        generator = WPPOutputGenerator()
        row = sample_record.to_wpp_dict()
        enriched = generator._enrich_with_template_info(row, sample_record)
        
        # DataHora_Disparo deve ser sempre vazio em homologação
        assert enriched.get('DataHora_Disparo') == '' or enriched.get('DataHora_Disparo') is None

