"""
Monitor de pasta para processamento automático de arquivos CSV
Utiliza watchdog para monitorar mudanças em diretórios
"""
import logging
import time
import shutil
from pathlib import Path
from typing import Optional, Set, Callable
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, FileMovedEvent

from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.utils import CSVParser

logger = logging.getLogger(__name__)


class CSVFileHandler(FileSystemEventHandler):
    """Handler para eventos de arquivos CSV"""
    
    def __init__(
        self,
        engine: QiggerDecisionEngine,
        db_manager: DatabaseManager,
        processed_files: Set[str],
        processed_folder: Optional[Path] = None,
        error_folder: Optional[Path] = None,
        on_file_processed: Optional[Callable] = None
    ):
        """
        Inicializa o handler de arquivos CSV
        
        Args:
            engine: Engine de decisão
            db_manager: Gerenciador de banco de dados
            processed_files: Set de arquivos já processados
            processed_folder: Pasta para mover arquivos processados (opcional)
            error_folder: Pasta para mover arquivos com erro (opcional)
            on_file_processed: Callback chamado quando arquivo é processado
        """
        self.engine = engine
        self.db_manager = db_manager
        self.processed_files = processed_files
        self.processed_folder = processed_folder
        self.error_folder = error_folder
        self.on_file_processed = on_file_processed
        
        # Criar pastas se necessário
        if self.processed_folder:
            self.processed_folder.mkdir(parents=True, exist_ok=True)
        if self.error_folder:
            self.error_folder.mkdir(parents=True, exist_ok=True)
    
    def on_created(self, event: FileSystemEvent):
        """Chamado quando um arquivo é criado"""
        if event.is_directory:
            return
        
        self._process_file(event.src_path)
    
    def on_moved(self, event: FileSystemEvent):
        """Chamado quando um arquivo é movido"""
        if event.is_directory:
            return
        
        # Processar o arquivo no novo local
        if isinstance(event, FileMovedEvent):
            self._process_file(event.dest_path)
        else:
            # Fallback para outros tipos de eventos
            self._process_file(event.src_path)
    
    def _process_file(self, file_path: str):
        """
        Processa um arquivo CSV
        
        Args:
            file_path: Caminho para o arquivo
        """
        file_path_obj = Path(file_path)
        
        # Verificar se é arquivo CSV
        if not file_path_obj.suffix.lower() == '.csv':
            logger.debug(f"Ignorando arquivo não-CSV: {file_path}")
            return
        
        # Verificar se já foi processado
        if str(file_path_obj.absolute()) in self.processed_files:
            logger.debug(f"Arquivo já processado: {file_path}")
            return
        
        # Aguardar um pouco para garantir que o arquivo está completamente escrito
        time.sleep(1)
        
        # Verificar se arquivo existe e não está vazio
        if not file_path_obj.exists():
            logger.warning(f"Arquivo não encontrado: {file_path}")
            return
        
        if file_path_obj.stat().st_size == 0:
            logger.warning(f"Arquivo vazio: {file_path}")
            return
        
        logger.info(f"Novo arquivo CSV detectado: {file_path}")
        
        try:
            # Parse do CSV
            records = CSVParser.parse_file(str(file_path_obj))
            logger.info(f"Parseados {len(records)} registros do arquivo {file_path_obj.name}")
            
            # Processar cada registro
            total_processed = 0
            total_errors = 0
            
            for i, record in enumerate(records, 1):
                try:
                    results = self.engine.process_record(record)
                    
                    if results:
                        logger.debug(
                            f"  Registro {i}/{len(records)}: "
                            f"{len(results)} regra(s) aplicada(s)"
                        )
                    
                    total_processed += 1
                    
                except Exception as e:
                    logger.error(f"Erro ao processar registro {i}: {e}")
                    total_errors += 1
            
            # Marcar como processado
            self.processed_files.add(str(file_path_obj.absolute()))
            
            logger.info(
                f"Arquivo processado com sucesso: {file_path_obj.name} "
                f"({total_processed} registros, {total_errors} erros)"
            )
            
            # Mover para pasta de processados
            if self.processed_folder:
                self._move_file(file_path_obj, self.processed_folder)
            
            # Callback
            if self.on_file_processed:
                self.on_file_processed(file_path_obj, total_processed, total_errors)
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_path}: {e}")
            
            # Mover para pasta de erros
            if self.error_folder:
                self._move_file(file_path_obj, self.error_folder)
    
    def _move_file(self, file_path: Path, destination_folder: Path):
        """
        Move arquivo para pasta de destino
        
        Args:
            file_path: Caminho do arquivo
            destination_folder: Pasta de destino
        """
        try:
            # Criar nome único com timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
            destination = destination_folder / new_name
            
            shutil.move(str(file_path), str(destination))
            logger.info(f"Arquivo movido para: {destination}")
            
        except Exception as e:
            logger.error(f"Erro ao mover arquivo {file_path}: {e}")


class FolderMonitor:
    """
    Monitor de pasta para processamento automático de arquivos CSV
    """
    
    def __init__(
        self,
        watch_folder: str,
        db_path: str = "data/portabilidade.db",
        processed_folder: Optional[str] = None,
        error_folder: Optional[str] = None,
        recursive: bool = True
    ):
        """
        Inicializa o monitor de pasta
        
        Args:
            watch_folder: Pasta a ser monitorada
            db_path: Caminho para o banco de dados
            processed_folder: Pasta para arquivos processados (opcional)
            error_folder: Pasta para arquivos com erro (opcional)
            recursive: Monitorar subpastas recursivamente
        """
        self.watch_folder = Path(watch_folder)
        self.db_path = db_path
        self.processed_folder = Path(processed_folder) if processed_folder else None
        self.error_folder = Path(error_folder) if error_folder else None
        self.recursive = recursive
        
        # Verificar se pasta existe
        if not self.watch_folder.exists():
            raise ValueError(f"Pasta não encontrada: {watch_folder}")
        
        if not self.watch_folder.is_dir():
            raise ValueError(f"Caminho não é uma pasta: {watch_folder}")
        
        # Inicializar componentes
        self.db_manager = DatabaseManager(db_path)
        self.engine = QiggerDecisionEngine(self.db_manager)
        
        # Set de arquivos processados
        self.processed_files: Set[str] = set()
        
        # Observer
        self.observer: Optional[Observer] = None
        self.is_running = False
    
    def start(self):
        """Inicia o monitoramento"""
        if self.is_running:
            logger.warning("Monitor já está em execução")
            return
        
        logger.info(f"Iniciando monitoramento da pasta: {self.watch_folder}")
        
        # Criar handler
        event_handler = CSVFileHandler(
            engine=self.engine,
            db_manager=self.db_manager,
            processed_files=self.processed_files,
            processed_folder=self.processed_folder,
            error_folder=self.error_folder,
            on_file_processed=self._on_file_processed
        )
        
        # Criar observer
        self.observer = Observer()
        self.observer.schedule(
            event_handler,
            str(self.watch_folder),
            recursive=self.recursive
        )
        
        # Iniciar observer
        self.observer.start()
        self.is_running = True
        
        logger.info("Monitor iniciado com sucesso")
        
        # Processar arquivos existentes na pasta
        self._process_existing_files()
    
    def stop(self):
        """Para o monitoramento"""
        if not self.is_running or not self.observer:
            logger.warning("Monitor não está em execução")
            return
        
        logger.info("Parando monitoramento...")
        
        self.observer.stop()
        self.observer.join()
        self.is_running = False
        
        logger.info("Monitor parado com sucesso")
    
    def _process_existing_files(self):
        """Processa arquivos CSV já existentes na pasta"""
        logger.info("Processando arquivos existentes na pasta...")
        
        csv_files = list(self.watch_folder.rglob("*.csv")) if self.recursive else list(self.watch_folder.glob("*.csv"))
        
        if not csv_files:
            logger.info("Nenhum arquivo CSV encontrado na pasta")
            return
        
        logger.info(f"Encontrados {len(csv_files)} arquivo(s) CSV para processar")
        
        for csv_file in csv_files:
            # Simular evento de criação
            event = type('Event', (), {
                'src_path': str(csv_file),
                'is_directory': False
            })()
            
            handler = CSVFileHandler(
                engine=self.engine,
                db_manager=self.db_manager,
                processed_files=self.processed_files,
                processed_folder=self.processed_folder,
                error_folder=self.error_folder,
                on_file_processed=self._on_file_processed
            )
            
            handler._process_file(str(csv_file))
    
    def _on_file_processed(self, file_path: Path, total_processed: int, total_errors: int):
        """
        Callback chamado quando um arquivo é processado
        
        Args:
            file_path: Caminho do arquivo
            total_processed: Total de registros processados
            total_errors: Total de erros
        """
        logger.info(
            f"Callback: Arquivo {file_path.name} processado - "
            f"{total_processed} registros, {total_errors} erros"
        )
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()

