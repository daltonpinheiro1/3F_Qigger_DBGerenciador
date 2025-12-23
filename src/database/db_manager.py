"""
Gerenciador de banco de dados para o sistema de portabilidade
Versão 2.0 - Com suporte a triggers.xlsx e melhorias de DBA
"""
import sqlite3
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from contextlib import contextmanager

from src.models.portabilidade import PortabilidadeRecord, TriggerRule

logger = logging.getLogger(__name__)

# Versão do schema do banco de dados
SCHEMA_VERSION = 3


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
        self._apply_performance_optimizations()
        self._initialize_database()
        self._check_and_migrate()
        self._create_all_indexes()
    
    def _ensure_db_directory(self):
        """Garante que o diretório do banco de dados existe"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def _apply_performance_optimizations(self):
        """Aplica otimizações de performance (DBA best practices)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Write-Ahead Logging para melhor concorrência
            cursor.execute("PRAGMA journal_mode = WAL")
            
            # Sincronização normal (seguro mas mais rápido que FULL)
            cursor.execute("PRAGMA synchronous = NORMAL")
            
            # Cache de 64MB para queries
            cursor.execute("PRAGMA cache_size = -64000")
            
            # Armazenar tabelas temporárias em memória
            cursor.execute("PRAGMA temp_store = MEMORY")
            
            # Habilitar mmap para leitura mais rápida
            cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB
            
            # Auto vacuum incremental
            cursor.execute("PRAGMA auto_vacuum = INCREMENTAL")
            
            conn.commit()
            logger.debug("Otimizações de performance aplicadas")
    
    def _initialize_database(self):
        """Inicializa as tabelas do banco de dados"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela de versão do schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de registros de portabilidade (nova estrutura)
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
                    responsavel_processamento TEXT,
                    data_inicial_processamento TEXT,
                    data_final_processamento TEXT,
                    registro_valido INTEGER,
                    -- Novos campos (triggers.xlsx)
                    regra_id INTEGER,
                    o_que_aconteceu TEXT,
                    acao_a_realizar TEXT,
                    tipo_mensagem TEXT,
                    template TEXT,
                    mapeado INTEGER DEFAULT 1,
                    novo_status_bilhete_trigger TEXT,
                    ajustes_numero_acesso_trigger TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(cpf, numero_acesso, numero_ordem)
                )
            """)
            
            # Tabela de regras de triggers (espelho do xlsx)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS triggers_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    regra_id INTEGER UNIQUE NOT NULL,
                    status_bilhete TEXT,
                    operadora_doadora TEXT,
                    motivo_recusa TEXT,
                    motivo_cancelamento TEXT,
                    ultimo_bilhete INTEGER,
                    motivo_nao_consultado TEXT,
                    novo_status_bilhete TEXT,
                    ajustes_numero_acesso TEXT,
                    o_que_aconteceu TEXT,
                    acao_a_realizar TEXT,
                    tipo_mensagem TEXT,
                    template TEXT,
                    ativo INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de histórico de decisões
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS decision_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    regra_id INTEGER,
                    rule_applied TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    o_que_aconteceu TEXT,
                    acao_a_realizar TEXT,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (record_id) REFERENCES portabilidade_records(id)
                )
            """)
            
            # Tabela de log de regras
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS rules_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    regra_id INTEGER,
                    rule_name TEXT NOT NULL,
                    rule_result TEXT NOT NULL,
                    execution_time_ms REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (record_id) REFERENCES portabilidade_records(id)
                )
            """)
            
            # Tabela de registros não mapeados (para análise)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS unmapped_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER,
                    status_bilhete TEXT,
                    operadora_doadora TEXT,
                    motivo_recusa TEXT,
                    motivo_cancelamento TEXT,
                    ultimo_bilhete INTEGER,
                    motivo_nao_consultado TEXT,
                    count INTEGER DEFAULT 1,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved INTEGER DEFAULT 0,
                    resolved_regra_id INTEGER,
                    FOREIGN KEY (record_id) REFERENCES portabilidade_records(id)
                )
            """)
            
            # Tabela de templates WPP (mapeamento de mensagens WhatsApp)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS templates_wpp (
                    id INTEGER PRIMARY KEY,
                    nome_modelo TEXT NOT NULL UNIQUE,
                    categoria TEXT,
                    cabecalho_texto TEXT,
                    corpo_mensagem TEXT,
                    rodape TEXT,
                    tipo_botao TEXT,
                    botao_texto TEXT,
                    botao_url TEXT,
                    variaveis TEXT,
                    ativo INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de mapeamento tipo_comunicacao -> template
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tipo_comunicacao_template (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo_comunicacao TEXT NOT NULL,
                    tipo_descricao TEXT,
                    template_id INTEGER NOT NULL,
                    ativo INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (template_id) REFERENCES templates_wpp(id),
                    UNIQUE(tipo_comunicacao)
                )
            """)
            
            conn.commit()
            logger.info("Banco de dados inicializado com sucesso")
    
    def _create_all_indexes(self):
        """Cria todos os índices após migrações (chamado no final da inicialização)"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            self._create_indexes(cursor)
            conn.commit()
    
    def _create_indexes(self, cursor):
        """Cria índices otimizados para performance"""
        # Índices principais para portabilidade_records
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
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_created_at 
            ON portabilidade_records(created_at)
        """)
        
        # Índice para decision_history
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_decision_record 
            ON decision_history(record_id, created_at)
        """)
        
        # Índices para novos campos (só cria se colunas existem)
        try:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_regra_id ON portabilidade_records(regra_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_unmapped 
                ON portabilidade_records(created_at) 
                WHERE mapeado = 0
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_tipo_mensagem 
                ON portabilidade_records(tipo_mensagem)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_acao 
                ON portabilidade_records(acao_a_realizar)
            """)
        except Exception as e:
            logger.debug(f"Alguns índices não puderam ser criados (normal em migração): {e}")
        
        # Índice composto para matching de regras
        try:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_rule_match 
                ON triggers_rules(status_bilhete, operadora_doadora, motivo_recusa)
            """)
        except Exception as e:
            logger.debug(f"Índice de triggers não criado: {e}")
        
        logger.debug("Índices criados/verificados")
    
    def _check_and_migrate(self):
        """Verifica e aplica migrações necessárias"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Verificar versão atual
            cursor.execute("SELECT MAX(version) FROM schema_version")
            result = cursor.fetchone()
            current_version = result[0] if result[0] else 0
            
            if current_version < SCHEMA_VERSION:
                self._run_migrations(cursor, current_version)
                cursor.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
                logger.info(f"Migração aplicada: v{current_version} -> v{SCHEMA_VERSION}")
            conn.commit()
    
    def _run_migrations(self, cursor, from_version: int):
        """Executa migrações de versão"""
        if from_version < 2:
            # Migração para v2: adicionar novas colunas
            self._migrate_to_v2(cursor)
        if from_version < 3:
            # Migração para v3: corrigir tabela rules_log e adicionar templates
            self._migrate_to_v3(cursor)
    
    def _migrate_to_v2(self, cursor):
        """Migração para versão 2 - adiciona novos campos"""
        # Verificar quais colunas já existem
        cursor.execute("PRAGMA table_info(portabilidade_records)")
        existing_columns = {col[1] for col in cursor.fetchall()}
        
        new_columns = [
            ("regra_id", "INTEGER"),
            ("o_que_aconteceu", "TEXT"),
            ("acao_a_realizar", "TEXT"),
            ("tipo_mensagem", "TEXT"),
            ("template", "TEXT"),
            ("mapeado", "INTEGER DEFAULT 1"),
            ("novo_status_bilhete_trigger", "TEXT"),
            ("ajustes_numero_acesso_trigger", "TEXT"),
        ]
        
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE portabilidade_records ADD COLUMN {col_name} {col_type}")
                    logger.info(f"Coluna {col_name} adicionada à tabela portabilidade_records")
                except sqlite3.OperationalError as e:
                    logger.warning(f"Coluna {col_name} já existe ou erro: {e}")
        
        # Atualizar registros existentes como mapeado=1 (assumindo que foram processados anteriormente)
        cursor.execute("UPDATE portabilidade_records SET mapeado = 1 WHERE mapeado IS NULL")
        
        # Adicionar colunas à decision_history se necessário
        cursor.execute("PRAGMA table_info(decision_history)")
        existing_dh_columns = {col[1] for col in cursor.fetchall()}
        
        dh_new_columns = [
            ("regra_id", "INTEGER"),
            ("o_que_aconteceu", "TEXT"),
            ("acao_a_realizar", "TEXT"),
        ]
        
        for col_name, col_type in dh_new_columns:
            if col_name not in existing_dh_columns:
                try:
                    cursor.execute(f"ALTER TABLE decision_history ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    pass
        
        logger.info("Migração v2 concluída")
    
    def _migrate_to_v3(self, cursor):
        """Migração para versão 3 - corrige tabela rules_log e adiciona suporte a templates"""
        # Verificar se coluna regra_id existe em rules_log
        cursor.execute("PRAGMA table_info(rules_log)")
        existing_columns = {col[1] for col in cursor.fetchall()}
        
        # Adicionar coluna regra_id se não existir
        if 'regra_id' not in existing_columns:
            try:
                cursor.execute("ALTER TABLE rules_log ADD COLUMN regra_id INTEGER")
                logger.info("Coluna regra_id adicionada à tabela rules_log")
            except sqlite3.OperationalError as e:
                logger.warning(f"Erro ao adicionar coluna regra_id: {e}")
        
        # Criar tabelas de templates WPP se não existirem
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS templates_wpp (
                id INTEGER PRIMARY KEY,
                nome_modelo TEXT NOT NULL UNIQUE,
                categoria TEXT,
                cabecalho_texto TEXT,
                corpo_mensagem TEXT,
                rodape TEXT,
                tipo_botao TEXT,
                botao_texto TEXT,
                botao_url TEXT,
                variaveis TEXT,
                ativo INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tipo_comunicacao_template (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo_comunicacao TEXT NOT NULL UNIQUE,
                tipo_descricao TEXT,
                template_id INTEGER,
                ativo INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (template_id) REFERENCES templates_wpp(id)
            )
        """)
        
        logger.info("Migração v3 concluída - rules_log corrigida e tabelas de templates criadas")
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexões com o banco de dados"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            # Habilitar foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
            conn.commit()
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro SQLite no banco de dados {self.db_path}: {e}", exc_info=True)
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Erro inesperado no banco de dados {self.db_path}: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_record(self, record: PortabilidadeRecord) -> int:
        """
        Insere um novo registro de portabilidade
        
        Args:
            record: Registro de portabilidade a ser inserido
            
        Returns:
            ID do registro inserido
        """
        if not record.cpf or not record.numero_acesso or not record.numero_ordem:
            raise ValueError("Campos obrigatórios ausentes: cpf, numero_acesso, numero_ordem são obrigatórios")
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            data = record.to_dict()
            
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO portabilidade_records (
                        cpf, numero_acesso, numero_ordem, codigo_externo,
                        numero_temporario, bilhete_temporario, numero_bilhete,
                        status_bilhete, operadora_doadora, data_portabilidade,
                        motivo_recusa, motivo_cancelamento, ultimo_bilhete,
                        status_ordem, preco_ordem, data_conclusao_ordem,
                        motivo_nao_consultado, responsavel_processamento,
                        data_inicial_processamento, data_final_processamento,
                        registro_valido, regra_id, o_que_aconteceu,
                        acao_a_realizar, tipo_mensagem, template, mapeado,
                        novo_status_bilhete_trigger, ajustes_numero_acesso_trigger
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
                    data['responsavel_processamento'],
                    data['data_inicial_processamento'], data['data_final_processamento'],
                    data['registro_valido'], data['regra_id'], data['o_que_aconteceu'],
                    data['acao_a_realizar'], data['tipo_mensagem'], data['template'],
                    data['mapeado'], data['novo_status_bilhete_trigger'],
                    data['ajustes_numero_acesso_trigger']
                ))
                
                record_id = cursor.lastrowid
                logger.debug(f"Registro inserido com ID: {record_id} (CPF: {record.cpf}, Ordem: {record.numero_ordem})")
                return record_id
            except sqlite3.IntegrityError as e:
                logger.warning(f"Violação de integridade ao inserir registro (CPF: {record.cpf}, Ordem: {record.numero_ordem}): {e}")
                cursor.execute("""
                    SELECT id FROM portabilidade_records
                    WHERE cpf = ? AND numero_acesso = ? AND numero_ordem = ?
                """, (record.cpf, record.numero_acesso, record.numero_ordem))
                row = cursor.fetchone()
                if row:
                    return row[0]
                raise
    
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
                    data['responsavel_processamento'],
                    data['data_inicial_processamento'], data['data_final_processamento'],
                    data['registro_valido'], data['regra_id'], data['o_que_aconteceu'],
                    data['acao_a_realizar'], data['tipo_mensagem'], data['template'],
                    data['mapeado'], data['novo_status_bilhete_trigger'],
                    data['ajustes_numero_acesso_trigger']
                ))
            
            # Executar inserção em lote
            cursor.executemany("""
                INSERT OR REPLACE INTO portabilidade_records (
                    cpf, numero_acesso, numero_ordem, codigo_externo,
                    numero_temporario, bilhete_temporario, numero_bilhete,
                    status_bilhete, operadora_doadora, data_portabilidade,
                    motivo_recusa, motivo_cancelamento, ultimo_bilhete,
                    status_ordem, preco_ordem, data_conclusao_ordem,
                    motivo_nao_consultado, responsavel_processamento,
                    data_inicial_processamento, data_final_processamento,
                    registro_valido, regra_id, o_que_aconteceu,
                    acao_a_realizar, tipo_mensagem, template, mapeado,
                    novo_status_bilhete_trigger, ajustes_numero_acesso_trigger
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
            
            # Buscar IDs inseridos
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
    
    def log_decision(self, record_id: int, rule_name: str, decision: str, 
                     details: str = "", regra_id: Optional[int] = None,
                     o_que_aconteceu: Optional[str] = None,
                     acao_a_realizar: Optional[str] = None):
        """
        Registra uma decisão tomada pela engine
        
        Args:
            record_id: ID do registro
            rule_name: Nome da regra aplicada
            decision: Decisão tomada
            details: Detalhes adicionais
            regra_id: ID da regra do triggers.xlsx
            o_que_aconteceu: O que aconteceu
            acao_a_realizar: Ação a ser realizada
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO decision_history 
                (record_id, regra_id, rule_applied, decision, o_que_aconteceu, acao_a_realizar, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (record_id, regra_id, rule_name, decision, o_que_aconteceu, acao_a_realizar, details))
            conn.commit()
    
    def log_rule_execution(self, record_id: int, rule_name: str, result: str, 
                          execution_time_ms: float, regra_id: Optional[int] = None):
        """
        Registra a execução de uma regra
        
        Args:
            record_id: ID do registro
            rule_name: Nome da regra
            result: Resultado da regra
            execution_time_ms: Tempo de execução em milissegundos
            regra_id: ID da regra do triggers.xlsx
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO rules_log (record_id, regra_id, rule_name, rule_result, execution_time_ms)
                VALUES (?, ?, ?, ?, ?)
            """, (record_id, regra_id, rule_name, result, execution_time_ms))
            conn.commit()
    
    def log_unmapped_record(self, record: PortabilidadeRecord, record_id: int):
        """
        Registra um registro não mapeado para análise
        
        Args:
            record: Registro não mapeado
            record_id: ID do registro na tabela principal
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            keys = record.get_matching_keys()
            
            # Verificar se combinação já existe
            cursor.execute("""
                SELECT id, count FROM unmapped_records 
                WHERE status_bilhete IS ? 
                  AND operadora_doadora IS ?
                  AND motivo_recusa IS ?
                  AND motivo_cancelamento IS ?
                  AND ultimo_bilhete IS ?
                  AND motivo_nao_consultado IS ?
                  AND resolved = 0
            """, (
                keys['status_bilhete'], keys['operadora_doadora'],
                keys['motivo_recusa'], keys['motivo_cancelamento'],
                keys['ultimo_bilhete'], keys['motivo_nao_consultado']
            ))
            
            row = cursor.fetchone()
            if row:
                # Atualizar contagem
                cursor.execute("""
                    UPDATE unmapped_records 
                    SET count = count + 1, last_seen = CURRENT_TIMESTAMP, record_id = ?
                    WHERE id = ?
                """, (record_id, row[0]))
            else:
                # Inserir novo
                cursor.execute("""
                    INSERT INTO unmapped_records 
                    (record_id, status_bilhete, operadora_doadora, motivo_recusa,
                     motivo_cancelamento, ultimo_bilhete, motivo_nao_consultado)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    record_id, keys['status_bilhete'], keys['operadora_doadora'],
                    keys['motivo_recusa'], keys['motivo_cancelamento'],
                    keys['ultimo_bilhete'], keys['motivo_nao_consultado']
                ))
            
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

    def get_unmapped_records(self) -> List[Dict[str, Any]]:
        """
        Retorna registros não mapeados agrupados
        
        Returns:
            Lista de combinações não mapeadas com contagem
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM unmapped_records 
                WHERE resolved = 0 
                ORDER BY count DESC, last_seen DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_records_by_regra(self, regra_id: int) -> List[Dict[str, Any]]:
        """
        Busca registros por ID de regra
        
        Args:
            regra_id: ID da regra
            
        Returns:
            Lista de registros
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM portabilidade_records 
                WHERE regra_id = ?
                ORDER BY created_at DESC
            """, (regra_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_records_by_acao(self, acao: str) -> List[Dict[str, Any]]:
        """
        Busca registros por ação a realizar
        
        Args:
            acao: Ação a realizar
            
        Returns:
            Lista de registros
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM portabilidade_records 
                WHERE acao_a_realizar = ?
                ORDER BY created_at DESC
            """, (acao,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do banco de dados
        
        Returns:
            Dicionário com estatísticas
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total de registros
            cursor.execute("SELECT COUNT(*) FROM portabilidade_records")
            stats['total_registros'] = cursor.fetchone()[0]
            
            # Registros mapeados vs não mapeados
            cursor.execute("SELECT COUNT(*) FROM portabilidade_records WHERE mapeado = 1")
            stats['registros_mapeados'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM portabilidade_records WHERE mapeado = 0")
            stats['registros_nao_mapeados'] = cursor.fetchone()[0]
            
            # Por tipo de mensagem
            cursor.execute("""
                SELECT tipo_mensagem, COUNT(*) as count 
                FROM portabilidade_records 
                WHERE tipo_mensagem IS NOT NULL
                GROUP BY tipo_mensagem
                ORDER BY count DESC
            """)
            stats['por_tipo_mensagem'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Por ação
            cursor.execute("""
                SELECT acao_a_realizar, COUNT(*) as count 
                FROM portabilidade_records 
                WHERE acao_a_realizar IS NOT NULL
                GROUP BY acao_a_realizar
                ORDER BY count DESC
            """)
            stats['por_acao'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Por regra
            cursor.execute("""
                SELECT regra_id, COUNT(*) as count 
                FROM portabilidade_records 
                WHERE regra_id IS NOT NULL
                GROUP BY regra_id
                ORDER BY count DESC
                LIMIT 20
            """)
            stats['por_regra'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            return stats
    
    def sync_triggers_from_loader(self, rules: List[TriggerRule]):
        """
        Sincroniza regras do TriggerLoader com o banco de dados
        
        Args:
            rules: Lista de regras do TriggerLoader
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            for rule in rules:
                data = rule.to_dict()
                cursor.execute("""
                    INSERT OR REPLACE INTO triggers_rules (
                        regra_id, status_bilhete, operadora_doadora, motivo_recusa,
                        motivo_cancelamento, ultimo_bilhete, motivo_nao_consultado,
                        novo_status_bilhete, ajustes_numero_acesso, o_que_aconteceu,
                        acao_a_realizar, tipo_mensagem, template, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    data['regra_id'], data['status_bilhete'], data['operadora_doadora'],
                    data['motivo_recusa'], data['motivo_cancelamento'], data['ultimo_bilhete'],
                    data['motivo_nao_consultado'], data['novo_status_bilhete'],
                    data['ajustes_numero_acesso'], data['o_que_aconteceu'],
                    data['acao_a_realizar'], data['tipo_mensagem'], data['template']
                ))
            
            conn.commit()
            logger.info(f"Sincronizadas {len(rules)} regras com o banco de dados")
    
    def vacuum(self):
        """Executa VACUUM para otimizar o banco de dados"""
        with self._get_connection() as conn:
            conn.execute("VACUUM")
            logger.info("VACUUM executado no banco de dados")
    
    def analyze(self):
        """Executa ANALYZE para atualizar estatísticas dos índices"""
        with self._get_connection() as conn:
            conn.execute("ANALYZE")
            logger.info("ANALYZE executado no banco de dados")
    
    def optimize(self):
        """Executa otimizações de manutenção (VACUUM + ANALYZE)"""
        self.vacuum()
        self.analyze()
        logger.info("Otimização completa do banco de dados realizada")
    
    # ==================== TEMPLATES WPP ====================
    
    def insert_template_wpp(self, template_data: Dict[str, Any]) -> int:
        """
        Insere ou atualiza um template WPP
        
        Args:
            template_data: Dicionário com dados do template
            
        Returns:
            ID do template inserido/atualizado
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO templates_wpp (
                    id, nome_modelo, categoria, cabecalho_texto, corpo_mensagem,
                    rodape, tipo_botao, botao_texto, botao_url, variaveis,
                    ativo, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                template_data.get('id'),
                template_data.get('nome_modelo'),
                template_data.get('categoria'),
                template_data.get('cabecalho_texto'),
                template_data.get('corpo_mensagem'),
                template_data.get('rodape'),
                template_data.get('tipo_botao'),
                template_data.get('botao_texto'),
                template_data.get('botao_url'),
                template_data.get('variaveis'),
                template_data.get('ativo', 1)
            ))
            conn.commit()
            return template_data.get('id')
    
    def insert_tipo_comunicacao_mapping(self, tipo_comunicacao: str, tipo_descricao: str, template_id: int):
        """
        Insere mapeamento tipo_comunicacao -> template
        
        Args:
            tipo_comunicacao: Código do tipo de comunicação
            tipo_descricao: Descrição do tipo
            template_id: ID do template
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO tipo_comunicacao_template (
                    tipo_comunicacao, tipo_descricao, template_id, ativo
                ) VALUES (?, ?, ?, 1)
            """, (tipo_comunicacao, tipo_descricao, template_id))
            conn.commit()
    
    def get_all_templates_wpp(self) -> List[Dict[str, Any]]:
        """
        Retorna todos os templates WPP ativos
        
        Returns:
            Lista de templates
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM templates_wpp WHERE ativo = 1 ORDER BY id
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_template_by_id(self, template_id: int) -> Optional[Dict[str, Any]]:
        """
        Busca um template pelo ID
        
        Args:
            template_id: ID do template
            
        Returns:
            Dicionário com dados do template ou None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM templates_wpp WHERE id = ?
            """, (template_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_template_for_tipo_comunicacao(self, tipo_comunicacao: str) -> Optional[Dict[str, Any]]:
        """
        Busca o template para um tipo de comunicação
        
        Args:
            tipo_comunicacao: Código do tipo de comunicação
            
        Returns:
            Dicionário com dados do template ou None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.* FROM templates_wpp t
                INNER JOIN tipo_comunicacao_template m ON t.id = m.template_id
                WHERE m.tipo_comunicacao = ? AND m.ativo = 1 AND t.ativo = 1
            """, (tipo_comunicacao,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def sync_templates_from_config(self):
        """
        Sincroniza templates do módulo templates_wpp.py com o banco de dados
        """
        try:
            from src.utils.templates_wpp import TEMPLATES, TIPO_COMUNICACAO_PARA_TEMPLATE
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Inserir templates
                for template_id, config in TEMPLATES.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO templates_wpp (
                            id, nome_modelo, categoria, cabecalho_texto,
                            tipo_botao, botao_texto, botao_url, variaveis,
                            ativo, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
                    """, (
                        config.id,
                        config.nome_modelo,
                        config.categoria,
                        config.cabecalho,
                        'Call to Action' if config.tem_botao else None,
                        config.botao_texto,
                        config.botao_url,
                        ','.join(config.variaveis) if config.variaveis else None
                    ))
                
                # Inserir mapeamentos tipo_comunicacao -> template
                tipo_nomes = {
                    "1": "Portabilidade Agendada",
                    "2": "Portabilidade Antecipada",
                    "3": "Portabilidade Concluída",
                    "5": "Reagendar Portabilidade",
                    "6": "Portabilidade Pendente",
                    "14": "Aguardando Retirada",
                    "43": "Endereço Incorreto",
                }
                
                for tipo, template_id in TIPO_COMUNICACAO_PARA_TEMPLATE.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO tipo_comunicacao_template (
                            tipo_comunicacao, tipo_descricao, template_id, ativo
                        ) VALUES (?, ?, ?, 1)
                    """, (tipo, tipo_nomes.get(tipo, f"Tipo {tipo}"), template_id))
                
                conn.commit()
                logger.info(f"Sincronizados {len(TEMPLATES)} templates e {len(TIPO_COMUNICACAO_PARA_TEMPLATE)} mapeamentos")
                
        except Exception as e:
            logger.error(f"Erro ao sincronizar templates: {e}")
    
    def get_templates_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas dos templates WPP
        
        Returns:
            Dicionário com estatísticas
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total de templates
            cursor.execute("SELECT COUNT(*) FROM templates_wpp WHERE ativo = 1")
            stats['total_templates'] = cursor.fetchone()[0]
            
            # Total de mapeamentos
            cursor.execute("SELECT COUNT(*) FROM tipo_comunicacao_template WHERE ativo = 1")
            stats['total_mapeamentos'] = cursor.fetchone()[0]
            
            # Templates por categoria
            cursor.execute("""
                SELECT categoria, COUNT(*) as count 
                FROM templates_wpp 
                WHERE ativo = 1
                GROUP BY categoria
            """)
            stats['por_categoria'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            # Mapeamentos por template
            cursor.execute("""
                SELECT t.nome_modelo, COUNT(m.id) as count
                FROM templates_wpp t
                LEFT JOIN tipo_comunicacao_template m ON t.id = m.template_id AND m.ativo = 1
                WHERE t.ativo = 1
                GROUP BY t.id
            """)
            stats['mapeamentos_por_template'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            return stats
