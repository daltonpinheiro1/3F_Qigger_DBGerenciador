"""
Pool de Conexões SQLite para Melhor Performance
Implementa padrão de pool de conexões para reduzir overhead de conexão/desconexão

Baseado em melhores práticas de:
- AWS RDS Connection Pooling
- PostgreSQL pgbouncer
- Microsoft SQL Server Connection Pooling
"""
import sqlite3
import logging
import threading
from typing import Optional, Generator
from contextlib import contextmanager
from queue import Queue, Empty
from pathlib import Path
import time

logger = logging.getLogger(__name__)


class SQLiteConnectionPool:
    """
    Pool de conexões SQLite thread-safe.
    
    Características:
    - Reutilização de conexões (reduz overhead)
    - Thread-safe com locks
    - Timeout configurável
    - Verificação de saúde das conexões
    - Auto-scaling (conexões sob demanda até max_connections)
    
    Uso:
        pool = SQLiteConnectionPool("data/portabilidade.db", max_connections=5)
        with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM tabela")
    """
    
    def __init__(
        self,
        db_path: str,
        max_connections: int = 5,
        timeout: float = 30.0,
        check_same_thread: bool = False
    ):
        """
        Inicializa o pool de conexões.
        
        Args:
            db_path: Caminho para o banco de dados SQLite
            max_connections: Número máximo de conexões no pool
            timeout: Timeout para obter conexão (segundos)
            check_same_thread: Se False, permite uso entre threads
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        
        # Garantir que diretório existe
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Pool de conexões disponíveis
        self._pool: Queue = Queue(maxsize=max_connections)
        
        # Controle de conexões
        self._lock = threading.Lock()
        self._active_connections = 0
        self._total_created = 0
        self._total_reused = 0
        
        # Pré-criar algumas conexões
        self._initialize_pool()
        
        logger.info(f"Pool de conexões inicializado: {db_path} (max: {max_connections})")
    
    def _initialize_pool(self):
        """Pré-cria conexões iniciais no pool"""
        initial_connections = min(2, self.max_connections)
        for _ in range(initial_connections):
            conn = self._create_connection()
            if conn:
                self._pool.put(conn)
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """
        Cria uma nova conexão SQLite com otimizações.
        
        Returns:
            Conexão SQLite configurada ou None em caso de erro
        """
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                isolation_level='DEFERRED',
                check_same_thread=self.check_same_thread
            )
            conn.row_factory = sqlite3.Row
            
            # Aplicar otimizações de DBA
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL")
            cursor.execute("PRAGMA synchronous = NORMAL")
            cursor.execute("PRAGMA cache_size = -128000")  # 128MB
            cursor.execute("PRAGMA temp_store = MEMORY")
            cursor.execute("PRAGMA mmap_size = 536870912")  # 512MB
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.commit()
            
            self._total_created += 1
            logger.debug(f"Nova conexão criada (total: {self._total_created})")
            
            return conn
        except Exception as e:
            logger.error(f"Erro ao criar conexão: {e}")
            return None
    
    def _validate_connection(self, conn: sqlite3.Connection) -> bool:
        """
        Valida se uma conexão está saudável.
        
        Args:
            conn: Conexão a validar
            
        Returns:
            True se a conexão está funcional
        """
        try:
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Obtém uma conexão do pool (context manager).
        
        Yields:
            Conexão SQLite do pool
            
        Raises:
            TimeoutError: Se não conseguir obter conexão no timeout
        """
        conn = None
        from_pool = False
        
        try:
            # Tentar obter do pool primeiro
            try:
                conn = self._pool.get(timeout=self.timeout)
                from_pool = True
                
                # Validar conexão
                if not self._validate_connection(conn):
                    logger.warning("Conexão do pool inválida, criando nova")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                    from_pool = False
                else:
                    self._total_reused += 1
                    
            except Empty:
                # Pool vazio, verificar se pode criar mais
                pass
            
            # Se não obteve do pool, criar nova
            if conn is None:
                with self._lock:
                    if self._active_connections < self.max_connections:
                        conn = self._create_connection()
                        self._active_connections += 1
                    else:
                        # Aguardar conexão disponível
                        try:
                            conn = self._pool.get(timeout=self.timeout)
                            from_pool = True
                        except Empty:
                            raise TimeoutError(
                                f"Timeout ao obter conexão do pool após {self.timeout}s"
                            )
            
            if conn is None:
                raise RuntimeError("Não foi possível obter conexão do pool")
            
            yield conn
            conn.commit()
            
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        
        finally:
            # Retornar conexão ao pool
            if conn:
                try:
                    # Verificar se conexão ainda é válida
                    if self._validate_connection(conn):
                        # Retornar ao pool se houver espaço
                        try:
                            self._pool.put_nowait(conn)
                        except Exception:
                            # Pool cheio, fechar conexão
                            conn.close()
                            with self._lock:
                                self._active_connections -= 1
                    else:
                        conn.close()
                        with self._lock:
                            self._active_connections -= 1
                except Exception:
                    pass
    
    def get_stats(self) -> dict:
        """
        Retorna estatísticas do pool.
        
        Returns:
            Dict com estatísticas
        """
        return {
            'db_path': self.db_path,
            'max_connections': self.max_connections,
            'active_connections': self._active_connections,
            'pool_size': self._pool.qsize(),
            'total_created': self._total_created,
            'total_reused': self._total_reused,
            'reuse_rate': (
                self._total_reused / (self._total_created + self._total_reused) * 100
                if (self._total_created + self._total_reused) > 0 else 0
            )
        }
    
    def close_all(self):
        """Fecha todas as conexões do pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Exception:
                pass
        
        with self._lock:
            self._active_connections = 0
        
        logger.info(f"Pool fechado: {self.db_path}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()
        return False


# Singleton global para reuso
_pools: dict = {}
_pools_lock = threading.Lock()


def get_pool(db_path: str, max_connections: int = 5) -> SQLiteConnectionPool:
    """
    Obtém ou cria um pool de conexões para o banco especificado.
    
    Args:
        db_path: Caminho para o banco de dados
        max_connections: Número máximo de conexões
        
    Returns:
        Pool de conexões (singleton por db_path)
    """
    with _pools_lock:
        if db_path not in _pools:
            _pools[db_path] = SQLiteConnectionPool(db_path, max_connections)
        return _pools[db_path]


def close_all_pools():
    """Fecha todos os pools de conexão"""
    with _pools_lock:
        for pool in _pools.values():
            pool.close_all()
        _pools.clear()
