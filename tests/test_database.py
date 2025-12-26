"""
Testes para o DatabaseManager
Versão 2.0 - Testa nova estrutura com campos de triggers
"""
import pytest
import tempfile
import os
from datetime import datetime

from src.database import DatabaseManager
from src.models.portabilidade import (
    PortabilidadeRecord,
    PortabilidadeStatus,
    StatusOrdem,
    TriggerRule
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
            # Novos campos
            regra_id=1,
            o_que_aconteceu="CANCELADO A PEDIDO CLIENTE",
            acao_a_realizar="CANCELADO A PEDIDO CLIENTE",
            tipo_mensagem="NÃO ENVIAR",
            template="-",
            mapeado=True,
        )
    
    def test_insert_record(self, db_manager, sample_record):
        """Teste: Inserir registro no banco"""
        record_id = db_manager.insert_record(sample_record)
        assert record_id is not None
        assert record_id > 0
    
    def test_insert_record_com_novos_campos(self, db_manager, sample_record):
        """Teste: Inserir registro com novos campos de triggers"""
        record_id = db_manager.insert_record(sample_record)
        
        # Buscar e verificar novos campos
        record = db_manager.get_record(
            sample_record.cpf,
            sample_record.numero_acesso,
            sample_record.numero_ordem
        )
        
        assert record is not None
        assert record['regra_id'] == 1
        assert record['o_que_aconteceu'] == "CANCELADO A PEDIDO CLIENTE"
        assert record['acao_a_realizar'] == "CANCELADO A PEDIDO CLIENTE"
        assert record['tipo_mensagem'] == "NÃO ENVIAR"
        assert record['mapeado'] == 1
    
    def test_get_record(self, db_manager, sample_record):
        """Teste: Buscar registro do banco"""
        db_manager.insert_record(sample_record)
        
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
            record_id=record_id,
            rule_name="trigger_rule_1",
            decision="CANCELAR",
            details="Portabilidade cancelada pelo cliente",
            regra_id=1,
            o_que_aconteceu="CANCELADO A PEDIDO CLIENTE",
            acao_a_realizar="CANCELADO A PEDIDO CLIENTE"
        )
        
        assert True  # Se chegou aqui, não deu erro
    
    def test_log_rule_execution(self, db_manager, sample_record):
        """Teste: Registrar execução de regra"""
        record_id = db_manager.insert_record(sample_record)
        
        db_manager.log_rule_execution(
            record_id=record_id,
            rule_name="trigger_rule_1",
            result="CANCELAR",
            execution_time_ms=15.5,
            regra_id=1
        )
        
        assert True
    
    def test_log_unmapped_record(self, db_manager):
        """Teste: Registrar registro não mapeado"""
        record = PortabilidadeRecord(
            cpf="12345678901",
            numero_acesso="11987654321",
            numero_ordem="1-1234567890123",
            codigo_externo="250001234",
            status_bilhete=PortabilidadeStatus.SUSPENSA,
            operadora_doadora="OPERADORA_NOVA",
            mapeado=False,
        )
        record.mark_as_unmapped()
        
        record_id = db_manager.insert_record(record)
        db_manager.log_unmapped_record(record, record_id)
        
        # Verificar registro não mapeado
        unmapped = db_manager.get_unmapped_records()
        assert len(unmapped) > 0
    
    def test_get_all_records(self, db_manager, sample_record):
        """Teste: Buscar todos os registros"""
        db_manager.insert_record(sample_record)
        
        record2 = PortabilidadeRecord(
            cpf="98765432100",
            numero_acesso="11912345678",
            numero_ordem="1-9876543210987",
            codigo_externo="250005678"
        )
        db_manager.insert_record(record2)
        
        all_records = db_manager.get_all_records()
        assert len(all_records) >= 2
    
    def test_get_all_records_with_limit(self, db_manager, sample_record):
        """Teste: Buscar registros com limite"""
        for i in range(5):
            record = PortabilidadeRecord(
                cpf=f"1234567890{i}",
                numero_acesso=f"1198765432{i}",
                numero_ordem=f"1-123456789012{i}",
                codigo_externo=f"25000123{i}"
            )
            db_manager.insert_record(record)
        
        records = db_manager.get_all_records(limit=3)
        assert len(records) == 3

    def test_get_records_by_regra(self, db_manager, sample_record):
        """Teste: Buscar registros por regra"""
        db_manager.insert_record(sample_record)
        
        records = db_manager.get_records_by_regra(1)
        assert len(records) >= 1
        assert records[0]['regra_id'] == 1
    
    def test_get_records_by_acao(self, db_manager, sample_record):
        """Teste: Buscar registros por ação"""
        db_manager.insert_record(sample_record)
        
        records = db_manager.get_records_by_acao("CANCELADO A PEDIDO CLIENTE")
        assert len(records) >= 1
    
    def test_get_statistics(self, db_manager, sample_record):
        """Teste: Obter estatísticas"""
        db_manager.insert_record(sample_record)
        
        # Inserir registro não mapeado
        record2 = PortabilidadeRecord(
            cpf="98765432100",
            numero_acesso="11912345678",
            numero_ordem="1-9876543210987",
            codigo_externo="250005678",
            mapeado=False,
        )
        record2.mark_as_unmapped()
        db_manager.insert_record(record2)
        
        stats = db_manager.get_statistics()
        
        assert 'total_registros' in stats
        assert stats['total_registros'] >= 2
        assert 'registros_mapeados' in stats
        assert 'registros_nao_mapeados' in stats
    
    def test_sync_triggers_from_loader(self, db_manager):
        """Teste: Sincronizar triggers do loader"""
        rules = [
            TriggerRule(
                regra_id=1,
                status_bilhete="Portado",
                operadora_doadora="VIVO",
                o_que_aconteceu="BP FECHADO",
                acao_a_realizar="PARABENIZAÇÃO",
                tipo_mensagem="CONFIRMACAO",
            ),
            TriggerRule(
                regra_id=2,
                status_bilhete="Portabilidade Cancelada",
                operadora_doadora="CLARO",
                o_que_aconteceu="CANCELAMENTO",
                acao_a_realizar="REABERTURA",
                tipo_mensagem="LIBERACAO",
            ),
        ]
        
        db_manager.sync_triggers_from_loader(rules)
        
        # Não deve dar erro
        assert True
    
    def test_optimize(self, db_manager, sample_record):
        """Teste: Otimização do banco"""
        db_manager.insert_record(sample_record)
        
        # Não deve dar erro
        db_manager.optimize()
        assert True
    
    def test_vacuum(self, db_manager, sample_record):
        """Teste: VACUUM do banco"""
        db_manager.insert_record(sample_record)
        
        db_manager.vacuum()
        assert True
    
    def test_analyze(self, db_manager, sample_record):
        """Teste: ANALYZE do banco"""
        db_manager.insert_record(sample_record)
        
        db_manager.analyze()
        assert True
