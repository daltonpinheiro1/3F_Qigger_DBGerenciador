"""
Módulo centralizado de configuração de logging
Padrão DevOps/SRE para toda a aplicação
"""
import logging
import sys
import io
from pathlib import Path
from typing import Optional
from datetime import datetime

# Cores para terminal (ANSI codes)
COLORS = {
    'DEBUG': '\033[36m',      # Cyan
    'INFO': '\033[32m',       # Green
    'WARNING': '\033[33m',    # Yellow
    'ERROR': '\033[31m',      # Red
    'CRITICAL': '\033[35m',   # Magenta
    'RESET': '\033[0m'        # Reset
}


class ColoredFormatter(logging.Formatter):
    """Formatter com cores para terminal"""
    
    def format(self, record: logging.LogRecord) -> str:
        # Adicionar cor apenas para terminal (não para arquivo)
        if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
            color = COLORS.get(record.levelname, COLORS['RESET'])
            reset = COLORS['RESET']
            record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def setup_logging(
    log_level: str = 'INFO',
    log_file: Optional[str] = None,
    log_dir: str = 'logs',
    app_name: str = 'qigger',
    include_timestamp_in_filename: bool = True
) -> logging.Logger:
    """
    Configura logging centralizado para toda a aplicação.
    
    Args:
        log_level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Nome do arquivo de log (opcional, usa app_name se não especificado)
        log_dir: Diretório para arquivos de log
        app_name: Nome da aplicação
        include_timestamp_in_filename: Se True, adiciona timestamp ao nome do arquivo
        
    Returns:
        Logger configurado
    """
    # Criar diretório de logs
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Configurar nome do arquivo de log
    if log_file is None:
        if include_timestamp_in_filename:
            timestamp = datetime.now().strftime('%Y%m%d')
            log_file = f"{app_name}_{timestamp}.log"
        else:
            log_file = f"{app_name}.log"
    
    log_path = Path(log_dir) / log_file
    
    # Formato padrão para logs
    file_format = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    console_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    # Configurar handler para arquivo
    file_handler = logging.FileHandler(
        str(log_path),
        encoding='utf-8',
        mode='a'
    )
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(logging.Formatter(file_format))
    
    # Configurar handler para console (com suporte a Windows)
    if sys.platform == 'win32':
        try:
            console_handler = logging.StreamHandler(
                io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            )
        except Exception:
            console_handler = logging.StreamHandler(sys.stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)
    
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(ColoredFormatter(console_format))
    
    # Configurar logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remover handlers existentes para evitar duplicação
    root_logger.handlers.clear()
    
    # Adicionar novos handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Retornar logger do módulo
    logger = logging.getLogger(app_name)
    logger.info(f"Logging configurado: {log_path}")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger com o nome especificado.
    
    Args:
        name: Nome do logger (geralmente __name__)
        
    Returns:
        Logger configurado
    """
    return logging.getLogger(name)


class LogContextManager:
    """Context manager para logging de operações com tempo de execução"""
    
    def __init__(self, logger: logging.Logger, operation: str, level: int = logging.INFO):
        self.logger = logger
        self.operation = operation
        self.level = level
        self.start_time = None
        
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.log(self.level, f"Iniciando: {self.operation}")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        if exc_type is not None:
            self.logger.error(
                f"Erro em {self.operation}: {exc_val} (tempo: {elapsed:.2f}s)"
            )
            return False  # Propagar exceção
        
        self.logger.log(
            self.level,
            f"Concluído: {self.operation} (tempo: {elapsed:.2f}s)"
        )
        return False


# Códigos de saída padronizados (POSIX standard)
class ExitCodes:
    """Códigos de saída padronizados para scripts"""
    SUCCESS = 0
    GENERAL_ERROR = 1
    ARGUMENT_ERROR = 2
    DATABASE_ERROR = 3
    FILE_NOT_FOUND = 4
    PERMISSION_ERROR = 5
    NETWORK_ERROR = 6
    CONFIG_ERROR = 7
    VALIDATION_ERROR = 8
