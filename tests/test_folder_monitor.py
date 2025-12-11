"""
Testes para o FolderMonitor
"""
import pytest
import tempfile
import os
import time
from pathlib import Path
from unittest.mock import Mock, patch

from src.monitor import FolderMonitor, CSVFileHandler
from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus


class TestFolderMonitor:
    """Testes para o FolderMonitor"""
    
    @pytest.fixture
    def temp_folder(self):
        """Fixture para criar pasta temporária"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def db_manager(self):
        """Fixture para criar DatabaseManager temporário"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        manager = DatabaseManager(db_path)
        yield manager
        
        # Limpeza
        os.unlink(db_path)
    
    @pytest.fixture
    def engine(self, db_manager):
        """Fixture para criar QiggerDecisionEngine"""
        return QiggerDecisionEngine(db_manager)
    
    def test_folder_monitor_init(self, temp_folder, db_manager):
        """Teste: Inicialização do FolderMonitor"""
        monitor = FolderMonitor(
            watch_folder=str(temp_folder),
            db_path=db_manager.db_path
        )
        
        assert monitor.watch_folder == temp_folder
        assert monitor.is_running is False
    
    def test_folder_monitor_invalid_folder(self, db_manager):
        """Teste: Pasta inválida"""
        with pytest.raises(ValueError):
            FolderMonitor(
                watch_folder="/pasta/que/nao/existe",
                db_path=db_manager.db_path
            )
    
    def test_csv_file_handler_process_file(self, temp_folder, engine, db_manager):
        """Teste: Processamento de arquivo CSV"""
        # Criar arquivo CSV de teste
        csv_file = temp_folder / "test.csv"
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo,Status do bilhete
12345678901,11987654321,1-1234567890123,250001234,Portabilidade Cancelada"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        # Criar handler
        processed_files = set()
        handler = CSVFileHandler(
            engine=engine,
            db_manager=db_manager,
            processed_files=processed_files
        )
        
        # Processar arquivo
        handler._process_file(str(csv_file))
        
        # Verificar se foi processado
        assert str(csv_file.absolute()) in processed_files
    
    def test_csv_file_handler_ignore_non_csv(self, temp_folder, engine, db_manager):
        """Teste: Ignorar arquivos não-CSV"""
        # Criar arquivo de texto
        txt_file = temp_folder / "test.txt"
        txt_file.write_text("teste")
        
        processed_files = set()
        handler = CSVFileHandler(
            engine=engine,
            db_manager=db_manager,
            processed_files=processed_files
        )
        
        # Processar arquivo
        handler._process_file(str(txt_file))
        
        # Verificar que não foi processado
        assert str(txt_file.absolute()) not in processed_files
    
    def test_csv_file_handler_move_to_processed(self, temp_folder, engine, db_manager):
        """Teste: Mover arquivo para pasta de processados"""
        # Criar pastas
        processed_folder = temp_folder / "processed"
        processed_folder.mkdir()
        
        # Criar arquivo CSV
        csv_file = temp_folder / "test.csv"
        csv_content = """Cpf,Número de acesso,Número da ordem,Código externo
12345678901,11987654321,1-1234567890123,250001234"""
        csv_file.write_text(csv_content, encoding='utf-8')
        
        # Criar handler
        processed_files = set()
        handler = CSVFileHandler(
            engine=engine,
            db_manager=db_manager,
            processed_files=processed_files,
            processed_folder=processed_folder
        )
        
        # Processar arquivo
        handler._process_file(str(csv_file))
        
        # Verificar que arquivo foi movido
        assert not csv_file.exists()
        assert len(list(processed_folder.glob("*.csv"))) > 0
    
    @pytest.mark.slow
    def test_folder_monitor_start_stop(self, temp_folder, db_manager):
        """Teste: Iniciar e parar monitoramento"""
        monitor = FolderMonitor(
            watch_folder=str(temp_folder),
            db_path=db_manager.db_path
        )
        
        # Iniciar
        monitor.start()
        assert monitor.is_running is True
        
        # Aguardar um pouco
        time.sleep(0.5)
        
        # Parar
        monitor.stop()
        assert monitor.is_running is False
    
    def test_folder_monitor_context_manager(self, temp_folder, db_manager):
        """Teste: Uso como context manager"""
        with FolderMonitor(
            watch_folder=str(temp_folder),
            db_path=db_manager.db_path
        ) as monitor:
            assert monitor.is_running is True
        
        # Verificar que parou ao sair do contexto
        assert monitor.is_running is False
    
    def test_process_existing_files(self, temp_folder, db_manager):
        """Teste: Processar arquivos existentes"""
        # Criar arquivos CSV
        for i in range(3):
            csv_file = temp_folder / f"test_{i}.csv"
            csv_content = f"""Cpf,Número de acesso,Número da ordem,Código externo
1234567890{i},1198765432{i},1-123456789012{i},25000123{i}"""
            csv_file.write_text(csv_content, encoding='utf-8')
        
        monitor = FolderMonitor(
            watch_folder=str(temp_folder),
            db_path=db_manager.db_path
        )
        
        # Processar arquivos existentes
        monitor._process_existing_files()
        
        # Verificar que foram processados
        assert len(monitor.processed_files) == 3

