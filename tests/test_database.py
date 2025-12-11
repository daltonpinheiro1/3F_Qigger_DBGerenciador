"""
Testes para o DatabaseManager
"""
import pytest
import tempfile
import os
from datetime import datetime

from src.database import DatabaseManager
from src.models.portabilidade import (
    PortabilidadeRecord,
    PortabilidadeStatus,
    StatusOrdem
)


class TestDatabaseManager:
    """Testes para o DatabaseManager"""
    
    @pytest.fixture
    def db_manager(self):
        """Fixture para criar um DatabaseManager temporário"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        manager = DatabaseManager(db_path)
        yield manager
        
        # Limpeza
        os.unlink(db_path)
    
    @pytest.fixture
    def sample_record(self):
        """Fixture para criar um registro de exemplo"""
        return PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.CANCELADA,
            operadora_doadora="VIVO",
            data_portabilidade=datetime(2025, 12, 10, 14, 0, 0),
            motivo_cancelamento="Cancelamento pelo Cliente",
            status_ordem=StatusOrdem.CONCLUIDO,
            preco_ordem="R$29,99",
            registro_valido=True,
            numero_acesso_valido=True
        )
    
    def test_insert_record(self, db_manager, sample_record):
        """Teste: Inserir registro no banco"""
        record_id = db_manager.insert_record(sample_record)
        assert record_id is not None
        assert record_id > 0
    
    def test_get_record(self, db_manager, sample_record):
        """Teste: Buscar registro do banco"""
        # Inserir primeiro
        db_manager.insert_record(sample_record)
        
        # Buscar
        record = db_manager.get_record(
            sample_record.cpf,
            sample_record.numero_acesso,
            sample_record.numero_ordem
        )
        
        assert record is not None
        assert record['cpf'] == sample_record.cpf
        assert record['numero_acesso'] == sample_record.numero_acesso
    
    def test_log_decision(self, db_manager, sample_record):
        """Teste: Registrar decisão no banco"""
        record_id = db_manager.insert_record(sample_record)
        
        db_manager.log_decision(
            record_id,
            "rule_05_portabilidade_cancelada",
            "CANCELAR",
            "Portabilidade cancelada pelo cliente"
        )
        
        # Verificar se foi inserido (não há método de busca, mas não deve dar erro)
        assert True  # Se chegou aqui, não deu erro
    
    def test_log_rule_execution(self, db_manager, sample_record):
        """Teste: Registrar execução de regra"""
        record_id = db_manager.insert_record(sample_record)
        
        db_manager.log_rule_execution(
            record_id,
            "rule_05_portabilidade_cancelada",
            "CANCELAR",
            15.5
        )
        
        # Verificar se foi inserido
        assert True
    
    def test_get_all_records(self, db_manager, sample_record):
        """Teste: Buscar todos os registros"""
        # Inserir alguns registros
        db_manager.insert_record(sample_record)
        
        # Criar segundo registro
        record2 = PortabilidadeRecord(
            cpf="98765432100",
            numero_acesso="11912345678",
            numero_ordem="1-9876543210987",
            codigo_externo="250005678"
        )
        db_manager.insert_record(record2)
        
        # Buscar todos
        all_records = db_manager.get_all_records()
        assert len(all_records) >= 2
    
    def test_get_all_records_with_limit(self, db_manager, sample_record):
        """Teste: Buscar registros com limite"""
        # Inserir múltiplos registros
        for i in range(5):
            record = PortabilidadeRecord(
                cpf=f"1234567890{i}",
                numero_acesso=f"1198765432{i}",
                numero_ordem=f"1-123456789012{i}",
                codigo_externo=f"25000123{i}"
            )
            db_manager.insert_record(record)
        
        # Buscar com limite
        records = db_manager.get_all_records(limit=3)
        assert len(records) == 3

