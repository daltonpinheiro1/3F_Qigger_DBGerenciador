"""
Gerenciador de banco de dados para o sistema de portabilidade
"""
import sqlite3
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from contextlib import contextmanager

from src.models.portabilidade import PortabilidadeRecord

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Gerenciador de banco de dados SQLite para portabilidade"""
    
    def __init__(self, db_path: str = "data/portabilidade.db"):
        """
        Inicializa o gerenciador de banco de dados
        
        Args:
            db_path: Caminho para o arquivo do banco de dados
        """
        self.db_path = db_path
        self._ensure_db_directory()
        self._initialize_database()
    
    def _ensure_db_directory(self):
        """Garante que o diretório do banco de dados existe"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _initialize_database(self):
        """Inicializa as tabelas do banco de dados"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela de registros de portabilidade
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portabilidade_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cpf TEXT NOT NULL,
                    numero_acesso TEXT NOT NULL,
                    numero_ordem TEXT NOT NULL,
                    codigo_externo TEXT NOT NULL,
                    numero_temporario TEXT,
                    bilhete_temporario TEXT,
                    numero_bilhete TEXT,
                    status_bilhete TEXT,
                    operadora_doadora TEXT,
                    data_portabilidade TEXT,
                    motivo_recusa TEXT,
                    motivo_cancelamento TEXT,
                    ultimo_bilhete INTEGER,
                    status_ordem TEXT,
                    preco_ordem TEXT,
                    data_conclusao_ordem TEXT,
                    motivo_nao_consultado TEXT,
                    motivo_nao_cancelado TEXT,
                    motivo_nao_aberto TEXT,
                    motivo_nao_reagendado TEXT,
                    novo_status_bilhete TEXT,
                    nova_data_portabilidade TEXT,
                    responsavel_processamento TEXT,
                    data_inicial_processamento TEXT,
                    data_final_processamento TEXT,
                    registro_valido INTEGER,
                    ajustes_registro TEXT,
                    numero_acesso_valido INTEGER,
                    ajustes_numero_acesso TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(cpf, numero_acesso, numero_ordem)
                )
            """)
            
            # Tabela de histórico de decisões
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS decision_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    rule_applied TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (record_id) REFERENCES portabilidade_records(id)
                )
            """)
            
            # Tabela de regras aplicadas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rules_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    rule_name TEXT NOT NULL,
                    rule_result TEXT NOT NULL,
                    execution_time_ms REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (record_id) REFERENCES portabilidade_records(id)
                )
            """)
            
            # Índices para melhor performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cpf ON portabilidade_records(cpf)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_numero_acesso ON portabilidade_records(numero_acesso)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_numero_ordem ON portabilidade_records(numero_ordem)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_status_bilhete ON portabilidade_records(status_bilhete)
            """)
            
            conn.commit()
            logger.info("Banco de dados inicializado com sucesso")
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexões com o banco de dados"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro no banco de dados: {e}")
            raise
        finally:
            conn.close()
    
    def insert_record(self, record: PortabilidadeRecord) -> int:
        """
        Insere um novo registro de portabilidade
        
        Args:
            record: Registro de portabilidade a ser inserido
            
        Returns:
            ID do registro inserido
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            data = record.to_dict()
            
            cursor.execute("""
                INSERT OR REPLACE INTO portabilidade_records (
                    cpf, numero_acesso, numero_ordem, codigo_externo,
                    numero_temporario, bilhete_temporario, numero_bilhete,
                    status_bilhete, operadora_doadora, data_portabilidade,
                    motivo_recusa, motivo_cancelamento, ultimo_bilhete,
                    status_ordem, preco_ordem, data_conclusao_ordem,
                    motivo_nao_consultado, motivo_nao_cancelado,
                    motivo_nao_aberto, motivo_nao_reagendado,
                    novo_status_bilhete, nova_data_portabilidade,
                    responsavel_processamento, data_inicial_processamento,
                    data_final_processamento, registro_valido,
                    ajustes_registro, numero_acesso_valido, ajustes_numero_acesso
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['cpf'], data['numero_acesso'], data['numero_ordem'],
                data['codigo_externo'], data['numero_temporario'],
                data['bilhete_temporario'], data['numero_bilhete'],
                data['status_bilhete'], data['operadora_doadora'],
                data['data_portabilidade'], data['motivo_recusa'],
                data['motivo_cancelamento'], data['ultimo_bilhete'],
                data['status_ordem'], data['preco_ordem'],
                data['data_conclusao_ordem'], data['motivo_nao_consultado'],
                data['motivo_nao_cancelado'], data['motivo_nao_aberto'],
                data['motivo_nao_reagendado'], data['novo_status_bilhete'],
                data['nova_data_portabilidade'], data['responsavel_processamento'],
                data['data_inicial_processamento'], data['data_final_processamento'],
                data['registro_valido'], data['ajustes_registro'],
                data['numero_acesso_valido'], data['ajustes_numero_acesso']
            ))
            
            record_id = cursor.lastrowid
            conn.commit()
            logger.debug(f"Registro inserido com ID: {record_id}")
            return record_id
    
    def insert_records_batch(self, records: List[PortabilidadeRecord]) -> List[int]:
        """
        Insere múltiplos registros em lote (otimização de performance)
        
        Args:
            records: Lista de registros de portabilidade
            
        Returns:
            Lista de IDs dos registros inseridos
        """
        if not records:
            return []
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            record_ids = []
            
            # Preparar dados em lote
            batch_data = []
            for record in records:
                data = record.to_dict()
                batch_data.append((
                    data['cpf'], data['numero_acesso'], data['numero_ordem'],
                    data['codigo_externo'], data['numero_temporario'],
                    data['bilhete_temporario'], data['numero_bilhete'],
                    data['status_bilhete'], data['operadora_doadora'],
                    data['data_portabilidade'], data['motivo_recusa'],
                    data['motivo_cancelamento'], data['ultimo_bilhete'],
                    data['status_ordem'], data['preco_ordem'],
                    data['data_conclusao_ordem'], data['motivo_nao_consultado'],
                    data['motivo_nao_cancelado'], data['motivo_nao_aberto'],
                    data['motivo_nao_reagendado'], data['novo_status_bilhete'],
                    data['nova_data_portabilidade'], data['responsavel_processamento'],
                    data['data_inicial_processamento'], data['data_final_processamento'],
                    data['registro_valido'], data['ajustes_registro'],
                    data['numero_acesso_valido'], data['ajustes_numero_acesso']
                ))
            
            # Executar inserção em lote
            cursor.executemany("""
                INSERT OR REPLACE INTO portabilidade_records (
                    cpf, numero_acesso, numero_ordem, codigo_externo,
                    numero_temporario, bilhete_temporario, numero_bilhete,
                    status_bilhete, operadora_doadora, data_portabilidade,
                    motivo_recusa, motivo_cancelamento, ultimo_bilhete,
                    status_ordem, preco_ordem, data_conclusao_ordem,
                    motivo_nao_consultado, motivo_nao_cancelado,
                    motivo_nao_aberto, motivo_nao_reagendado,
                    novo_status_bilhete, nova_data_portabilidade,
                    responsavel_processamento, data_inicial_processamento,
                    data_final_processamento, registro_valido,
                    ajustes_registro, numero_acesso_valido, ajustes_numero_acesso
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
            
            # Buscar IDs inseridos (usando os campos únicos)
            record_ids = []
            for record in records:
                cursor.execute("""
                    SELECT id FROM portabilidade_records
                    WHERE cpf = ? AND numero_acesso = ? AND numero_ordem = ?
                """, (record.cpf, record.numero_acesso, record.numero_ordem))
                row = cursor.fetchone()
                if row:
                    record_ids.append(row[0])
            
            logger.debug(f"Inseridos {len(records)} registros em lote")
            return record_ids
    
    def get_record(self, cpf: str, numero_acesso: str, numero_ordem: str) -> Optional[Dict[str, Any]]:
        """
        Busca um registro específico
        
        Args:
            cpf: CPF do cliente
            numero_acesso: Número de acesso
            numero_ordem: Número da ordem
            
        Returns:
            Dicionário com os dados do registro ou None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM portabilidade_records
                WHERE cpf = ? AND numero_acesso = ? AND numero_ordem = ?
            """, (cpf, numero_acesso, numero_ordem))
            
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def log_decision(self, record_id: int, rule_name: str, decision: str, details: str = ""):
        """
        Registra uma decisão tomada pela engine
        
        Args:
            record_id: ID do registro
            rule_name: Nome da regra aplicada
            decision: Decisão tomada
            details: Detalhes adicionais
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO decision_history (record_id, rule_applied, decision, details)
                VALUES (?, ?, ?, ?)
            """, (record_id, rule_name, decision, details))
            conn.commit()
    
    def log_rule_execution(self, record_id: int, rule_name: str, result: str, execution_time_ms: float):
        """
        Registra a execução de uma regra
        
        Args:
            record_id: ID do registro
            rule_name: Nome da regra
            result: Resultado da regra
            execution_time_ms: Tempo de execução em milissegundos
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO rules_log (record_id, rule_name, rule_result, execution_time_ms)
                VALUES (?, ?, ?, ?)
            """, (record_id, rule_name, result, execution_time_ms))
            conn.commit()
    
    def get_all_records(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Busca todos os registros
        
        Args:
            limit: Limite de registros a retornar
            
        Returns:
            Lista de dicionários com os registros
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM portabilidade_records ORDER BY created_at DESC"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]

