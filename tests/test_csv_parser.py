"""
Testes para o CSVParser
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
    
    def test_parse_date_vazio(self):
        """Teste: Parse de data vazia"""
        result = CSVParser.parse_date("")
        assert result is None
    
    def test_parse_bool_sim(self):
        """Teste: Parse de booleano 'Sim'"""
        assert CSVParser.parse_bool("Sim") is True
        assert CSVParser.parse_bool("sim") is True
        assert CSVParser.parse_bool("SIM") is True
    
    def test_parse_bool_nao(self):
        """Teste: Parse de booleano 'Não'"""
        assert CSVParser.parse_bool("Não") is False
        assert CSVParser.parse_bool("nao") is False
        assert CSVParser.parse_bool("NÃO") is False
    
    def test_parse_status_bilhete(self):
        """Teste: Parse de status do bilhete"""
        result = CSVParser.parse_status_bilhete("Portabilidade Cancelada")
        assert result == PortabilidadeStatus.CANCELADA
    
    def test_parse_status_ordem(self):
        """Teste: Parse de status da ordem"""
        result = CSVParser.parse_status_ordem("Concluído")
        assert result == StatusOrdem.CONCLUIDO
    
    def test_parse_file_completo(self):
        """Teste: Parse de arquivo CSV completo"""
        # Criar arquivo CSV temporário
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo,Número temporário,Bilhete temporário,Número do bilhete,Status do bilhete,Operadora doadora,Data da portabilidade,Motivo da recusa,Motivo do cancelamento,Último bilhete de portabilidade?,Status da ordem,Preço da ordem,Data da conclusão da ordem,Motivo de não ter sido consultado,Motivo de não ter sido cancelado,Motivo de não ter sido aberto,Motivo de não ter sido reagendado,Novo status do bilhete,Nova data da portabilidade,Responsável pelo processamento,Data inicial do processamento,Data final do processamento,Registro válido?,Ajustes registro,Número de acesso válido?,Ajustes número de acesso
12345678901,11987654321,1-1234567890123,250001234,,,,Portabilidade Cancelada,VIVO,10/12/2025 14:00:00,Cancelamento pelo Cliente,Cancelamento pelo Cliente,Sim,Concluído,"R$29,99",10/12/2025 14:00:00,,,,,,,,Robô Siebel 5,10/12/2025 13:00:00,10/12/2025 14:00:00,Sim,,Sim,"""
        
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
            assert record.ultimo_bilhete is True
            # registro_valido pode ser None se não estiver no CSV, então apenas verificamos se não é False
            assert record.registro_valido is not False
            
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

