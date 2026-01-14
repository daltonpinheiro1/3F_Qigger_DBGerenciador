"""
Módulo centralizado de tratamento de erros
Padrão DevOps/SRE com classes de exceção customizadas
"""
import logging
import traceback
from typing import Optional, Any, Dict
from functools import wraps
import sys

logger = logging.getLogger(__name__)


# ==================== EXCEÇÕES CUSTOMIZADAS ====================

class QiggerBaseException(Exception):
    """Exceção base para todas as exceções do sistema Qigger"""
    
    def __init__(self, message: str, code: int = 1, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.code = code
        self.details = details or {}
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'error': self.__class__.__name__,
            'message': self.message,
            'code': self.code,
            'details': self.details
        }


class DatabaseError(QiggerBaseException):
    """Erro relacionado ao banco de dados"""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code=3, details=details)


class ValidationError(QiggerBaseException):
    """Erro de validação de dados"""
    def __init__(self, message: str, field: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if field:
            details['field'] = field
        super().__init__(message, code=8, details=details)


class FileOperationError(QiggerBaseException):
    """Erro em operações de arquivo"""
    def __init__(self, message: str, file_path: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if file_path:
            details['file_path'] = file_path
        super().__init__(message, code=4, details=details)


class ConfigurationError(QiggerBaseException):
    """Erro de configuração"""
    def __init__(self, message: str, config_key: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if config_key:
            details['config_key'] = config_key
        super().__init__(message, code=7, details=details)


class ProcessingError(QiggerBaseException):
    """Erro durante processamento de dados"""
    def __init__(self, message: str, record_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        details = details or {}
        if record_id:
            details['record_id'] = record_id
        super().__init__(message, code=1, details=details)


# ==================== DECORADORES ====================

def handle_exceptions(
    default_return: Any = None,
    log_level: int = logging.ERROR,
    reraise: bool = False,
    exception_types: tuple = (Exception,)
):
    """
    Decorador para tratamento de exceções.
    
    Args:
        default_return: Valor a retornar em caso de exceção
        log_level: Nível de log para a exceção
        reraise: Se True, relança a exceção após logar
        exception_types: Tipos de exceção a tratar
        
    Example:
        @handle_exceptions(default_return=[], reraise=False)
        def process_data():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_types as e:
                logger.log(
                    log_level,
                    f"Erro em {func.__name__}: {e}",
                    exc_info=log_level >= logging.ERROR
                )
                if reraise:
                    raise
                return default_return
        return wrapper
    return decorator


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    exception_types: tuple = (Exception,)
):
    """
    Decorador para retry com backoff exponencial.
    
    Args:
        max_attempts: Número máximo de tentativas
        delay_seconds: Delay inicial entre tentativas
        backoff_factor: Fator de multiplicação do delay
        exception_types: Tipos de exceção que disparam retry
        
    Example:
        @retry(max_attempts=3, delay_seconds=1.0)
        def api_call():
            ...
    """
    import time
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay_seconds
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exception_types as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"Tentativa {attempt}/{max_attempts} falhou para {func.__name__}: {e}. "
                            f"Aguardando {current_delay:.1f}s antes de retry..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff_factor
                    else:
                        logger.error(
                            f"Todas as {max_attempts} tentativas falharam para {func.__name__}: {e}"
                        )
            
            raise last_exception
        return wrapper
    return decorator


# ==================== FUNÇÕES UTILITÁRIAS ====================

def safe_execute(
    func: callable,
    *args,
    default_return: Any = None,
    log_errors: bool = True,
    **kwargs
) -> Any:
    """
    Executa uma função de forma segura, capturando exceções.
    
    Args:
        func: Função a executar
        *args: Argumentos posicionais
        default_return: Valor a retornar em caso de erro
        log_errors: Se deve logar erros
        **kwargs: Argumentos nomeados
        
    Returns:
        Resultado da função ou default_return em caso de erro
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Erro em {func.__name__}: {e}")
            logger.debug(traceback.format_exc())
        return default_return


def exit_with_error(
    message: str,
    code: int = 1,
    logger_instance: Optional[logging.Logger] = None
) -> None:
    """
    Encerra o programa com erro, logando a mensagem.
    
    Args:
        message: Mensagem de erro
        code: Código de saída
        logger_instance: Logger específico a usar
    """
    log = logger_instance or logger
    log.error(f"Encerrando com erro ({code}): {message}")
    sys.exit(code)


def format_exception_details(e: Exception) -> Dict[str, Any]:
    """
    Formata detalhes de uma exceção para logging estruturado.
    
    Args:
        e: Exceção a formatar
        
    Returns:
        Dict com detalhes da exceção
    """
    return {
        'type': type(e).__name__,
        'message': str(e),
        'traceback': traceback.format_exc(),
        'args': e.args
    }
