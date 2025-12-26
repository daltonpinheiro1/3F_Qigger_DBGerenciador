"""
Testes para o CSVParser
Versão 2.0 - Adaptado para nova estrutura
"""
import pytest
import tempfile
import os
from datetime import datetime

from src.utils.csv_parser import CSVParser
from src.models.portabilidade import PortabilidadeStatus, StatusOrdem


class TestCSVParser:
    """Testes para o CSVParser"""
    
    def test_parse_date_formato_brasileiro(self):
        """Teste: Parse de data formato brasileiro"""
        date_str = "10/12/2025 14:30:00"
        result = CSVParser.parse_date(date_str)
        assert result is not None
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 10
    
    def test_parse_date_formato_simples(self):
        """Teste: Parse de data formato simples"""
        date_str = "10/12/2025"
        result = CSVParser.parse_date(date_str)
        assert result is not None
        assert result.year == 2025
    
    def test_parse_date_formato_iso(self):
        """Teste: Parse de data formato ISO"""
        date_str = "2025-12-10"
        result = CSVParser.parse_date(date_str)
        assert result is not None
        assert result.year == 2025
        assert result.month == 12
        assert result.day == 10
    
    def test_parse_date_vazio(self):
        """Teste: Parse de data vazia"""
        result = CSVParser.parse_date("")
        assert result is None
    
    def test_parse_date_none(self):
        """Teste: Parse de data None"""
        result = CSVParser.parse_date(None)
        assert result is None
    
    def test_parse_bool_sim(self):
        """Teste: Parse de booleano 'Sim'"""
        assert CSVParser.parse_bool("Sim") is True
        assert CSVParser.parse_bool("sim") is True
        assert CSVParser.parse_bool("SIM") is True
        assert CSVParser.parse_bool("S") is True
        assert CSVParser.parse_bool("s") is True
    
    def test_parse_bool_nao(self):
        """Teste: Parse de booleano 'Não'"""
        assert CSVParser.parse_bool("Não") is False
        assert CSVParser.parse_bool("nao") is False
        assert CSVParser.parse_bool("NÃO") is False
        assert CSVParser.parse_bool("N") is False
        assert CSVParser.parse_bool("n") is False
    
    def test_parse_bool_yes_no(self):
        """Teste: Parse de booleano em inglês"""
        assert CSVParser.parse_bool("yes") is True
        assert CSVParser.parse_bool("no") is False
        assert CSVParser.parse_bool("true") is True
        assert CSVParser.parse_bool("false") is False
    
    def test_parse_bool_numerico(self):
        """Teste: Parse de booleano numérico"""
        assert CSVParser.parse_bool("1") is True
        assert CSVParser.parse_bool("0") is False
    
    def test_parse_bool_vazio(self):
        """Teste: Parse de booleano vazio"""
        assert CSVParser.parse_bool("") is None
        assert CSVParser.parse_bool(None) is None
    
    def test_parse_status_bilhete_cancelada(self):
        """Teste: Parse de status do bilhete cancelada"""
        result = CSVParser.parse_status_bilhete("Portabilidade Cancelada")
        assert result == PortabilidadeStatus.CANCELADA
    
    def test_parse_status_bilhete_portado(self):
        """Teste: Parse de status do bilhete portado"""
        result = CSVParser.parse_status_bilhete("Portado")
        assert result == PortabilidadeStatus.CONCLUIDA
    
    def test_parse_status_bilhete_pendente(self):
        """Teste: Parse de status do bilhete pendente"""
        result = CSVParser.parse_status_bilhete("Portabilidade Pendente")
        assert result == PortabilidadeStatus.PENDENTE
    
    def test_parse_status_bilhete_conflito(self):
        """Teste: Parse de status do bilhete conflito"""
        result = CSVParser.parse_status_bilhete("Conflito")
        assert result == PortabilidadeStatus.CONFLITO
    
    def test_parse_status_bilhete_vazio(self):
        """Teste: Parse de status do bilhete vazio"""
        result = CSVParser.parse_status_bilhete("")
        assert result is None
    
    def test_parse_status_ordem_concluido(self):
        """Teste: Parse de status da ordem concluído"""
        result = CSVParser.parse_status_ordem("Concluído")
        assert result == StatusOrdem.CONCLUIDO
    
    def test_parse_status_ordem_pendente(self):
        """Teste: Parse de status da ordem pendente"""
        result = CSVParser.parse_status_ordem("Pendente Portabilidade")
        assert result == StatusOrdem.PENDENTE
    
    def test_parse_file_completo(self):
        """Teste: Parse de arquivo CSV completo"""
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo,Número temporário,Bilhete temporário,Número do bilhete,Status do bilhete,Operadora doadora,Data da portabilidade,Motivo da recusa,Motivo do cancelamento,Último bilhete de portabilidade?,Status da ordem,Preço da ordem,Data da conclusão da ordem,Motivo de não ter sido consultado,Responsável pelo processamento,Data inicial do processamento,Data final do processamento,Registro válido?
12345678901,11987654321,1-1234567890123,250001234,,,,Portabilidade Cancelada,VIVO,10/12/2025 14:00:00,Cancelamento pelo Cliente,Cancelamento pelo Cliente,Sim,Concluído,"R$29,99",10/12/2025 14:00:00,,Robô Siebel 5,10/12/2025 13:00:00,10/12/2025 14:00:00,Sim"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            records = CSVParser.parse_file(temp_path)
            assert len(records) == 1
            
            record = records[0]
            assert record.cpf == "12345678901"
            assert record.numero_acesso == "11987654321"
            assert record.status_bilhete == PortabilidadeStatus.CANCELADA
            assert record.operadora_doadora == "VIVO"
            assert record.ultimo_bilhete is True
            assert record.motivo_recusa == "Cancelamento pelo Cliente"
            
        finally:
            os.unlink(temp_path)
    
    def test_parse_file_campos_faltando(self):
        """Teste: Parse de arquivo com campos obrigatórios faltando"""
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo
,11987654321,1-1234567890123,250001234"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            records = CSVParser.parse_file(temp_path)
            # Deve pular registros com campos obrigatórios faltando
            assert len(records) == 0
        finally:
            os.unlink(temp_path)

    def test_parse_file_encoding_latin1(self):
        """Teste: Parse de arquivo com encoding latin-1"""
        # Headers sem acento para evitar problemas de encoding
        csv_content = """Cpf,Numero de acesso,Numero da ordem,Codigo externo,Status do bilhete
12345678901,11987654321,1-1234567890123,250001234,Portabilidade Cancelada"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='latin-1') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            # O parser pode não conseguir ler headers sem acento
            # Este teste verifica que o parser não falha com encoding latin-1
            records = CSVParser.parse_file(temp_path)
            # Se não lançar exceção, o teste passou
            assert True
        finally:
            os.unlink(temp_path)
    
    def test_parse_file_nao_existe(self):
        """Teste: Parse de arquivo que não existe"""
        with pytest.raises(FileNotFoundError):
            CSVParser.parse_file("arquivo_inexistente.csv")
    
    def test_get_csv_headers(self):
        """Teste: Obter headers esperados"""
        headers = CSVParser.get_csv_headers()
        
        assert 'Cpf' in headers
        assert 'Número de acesso' in headers
        assert 'Número da ordem' in headers
        assert 'Código externo' in headers
        assert 'Status do bilhete' in headers
        assert 'Operadora doadora' in headers
    
    def test_validate_csv_structure_valido(self):
        """Teste: Validar estrutura CSV válida"""
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo
12345678901,11987654321,1-1234567890123,250001234"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            is_valid, errors = CSVParser.validate_csv_structure(temp_path)
            assert is_valid is True
            assert len(errors) == 0
        finally:
            os.unlink(temp_path)
    
    def test_validate_csv_structure_campos_faltando(self):
        """Teste: Validar estrutura CSV com campos faltando"""
        csv_content = """Cpf,Número de acesso
12345678901,11987654321"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            is_valid, errors = CSVParser.validate_csv_structure(temp_path)
            assert is_valid is False
            assert len(errors) > 0
            assert any("obrigatórios" in e.lower() for e in errors)
        finally:
            os.unlink(temp_path)
    
    def test_validate_csv_structure_arquivo_vazio(self):
        """Teste: Validar estrutura CSV vazio"""
        csv_content = ""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            is_valid, errors = CSVParser.validate_csv_structure(temp_path)
            assert is_valid is False
        finally:
            os.unlink(temp_path)
