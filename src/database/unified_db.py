"""
Sistema Unificado de Banco de Dados para Acompanhamento TIM
Versão 1.0 - Com versionamento completo e histórico de atualizações

Este módulo implementa um sistema de banco de dados unificado que:
- Unifica dados de múltiplas fontes (Base Analítica, Relatório de Objetos, Gerenciador)
- Mantém histórico completo através de versionamento (nova linha para cada atualização)
- Foca em campos críticos: número da ordem, status da ordem, status logística, status do bilhete
"""

import sqlite3
import logging
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json

logger = logging.getLogger(__name__)

# Versão do schema unificado
UNIFIED_SCHEMA_VERSION = 1


class UnifiedDatabaseManager:
    """
    Gerenciador de banco de dados unificado com versionamento
    
    Características:
    - Versionamento: Cada atualização cria uma nova linha mantendo histórico
    - Unificação: Integra dados de múltiplas fontes
    - Rastreabilidade: Mantém data de armazenamento e origem dos dados
    """
    
    def __init__(self, db_path: str = "data/tim_unificado.db"):
        """
        Inicializa o gerenciador de banco unificado
        
        Args:
            db_path: Caminho para o arquivo do banco de dados
        """
        self.db_path = db_path
        self._ensure_db_directory()
        self._apply_performance_optimizations()
        self._initialize_unified_database()
        self._create_unified_indexes()
    
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
            
            # Cache de 128MB para queries
            cursor.execute("PRAGMA cache_size = -128000")
            
            # Armazenar tabelas temporárias em memória
            cursor.execute("PRAGMA temp_store = MEMORY")
            
            # Habilitar mmap para leitura mais rápida
            cursor.execute("PRAGMA mmap_size = 536870912")  # 512MB
            
            # Auto vacuum incremental
            cursor.execute("PRAGMA auto_vacuum = INCREMENTAL")
            
            conn.commit()
            logger.debug("Otimizações de performance aplicadas ao banco unificado")
    
    def _initialize_unified_database(self):
        """Inicializa a estrutura do banco unificado"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Tabela de versão do schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS unified_schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela principal unificada com versionamento
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tim_unificado (
                    -- ID único do registro (mantém o mesmo para todas as versões)
                    id_isize TEXT NOT NULL,
                    
                    -- ID interno (auto-incremento)
                    registro_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    
                    -- Versão do registro (incrementa a cada atualização)
                    versao INTEGER NOT NULL DEFAULT 1,
                    
                    -- Data de armazenamento desta versão
                    data_armazenamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Origem dos dados (fonte principal que gerou esta versão)
                    origem_dados TEXT, -- 'base_analitica', 'relatorio_objetos', 'gerenciador', 'siebel'
                    
                    -- ===== DADOS BÁSICOS (chaves de identificação) =====
                    cpf TEXT,
                    numero_acesso TEXT,
                    numero_ordem TEXT NOT NULL,  -- CHAVE PRINCIPAL DE RASTREAMENTO
                    codigo_externo TEXT,
                    proposta_isize TEXT,  -- ID iSize (pode ser igual ao codigo_externo)
                    
                    -- ===== DADOS DO CLIENTE =====
                    cliente_nome TEXT,
                    cliente_telefone TEXT,
                    telefone_portado TEXT,
                    numero_provisorio TEXT,
                    
                    -- ===== ENDEREÇO =====
                    endereco TEXT,
                    numero TEXT,
                    complemento TEXT,
                    bairro TEXT,
                    cidade TEXT,
                    uf TEXT,
                    cep TEXT,
                    ponto_referencia TEXT,
                    
                    -- ===== STATUS DA ORDEM (FOCO PRINCIPAL) =====
                    status_ordem TEXT,  -- Status atual da ordem
                    status_ordem_anterior TEXT,  -- Status anterior (para comparação)
                    order_status TEXT,  -- Status da ordem no sistema
                    data_conclusao_ordem TEXT,
                    
                    -- ===== STATUS LOGÍSTICA (FOCO PRINCIPAL) =====
                    status_logistica TEXT,  -- Status atual da logística
                    status_logistica_anterior TEXT,  -- Status anterior
                    status_log_real TEXT,  -- Status logística real
                    data_logistica TEXT,  -- Data da última atualização logística
                    tipo_entrega TEXT,  -- 'CORREIOS', 'EXPRESS', etc
                    pedido_blue TEXT,  -- ID do pedido Blue
                    data_envio TEXT,
                    qtd_envios INTEGER,
                    
                    -- ===== STATUS DO BILHETE (FOCO PRINCIPAL) =====
                    status_bilhete TEXT,  -- Status atual do bilhete
                    status_bilhete_anterior TEXT,  -- Status anterior
                    status_bilhete_trigger TEXT,  -- Status após trigger
                    data_bilhete TEXT,  -- Data do bilhete
                    numero_bilhete TEXT,
                    operadora_doadora TEXT,
                    
                    -- ===== PORTABILIDADE =====
                    data_portabilidade TEXT,
                    data_portabilidade_atual TEXT,
                    data_portabilidade_penult TEXT,
                    data_reagendamento_crm TEXT,
                    houve_reagendamento TEXT,  -- 'SIM' ou 'NÃO'
                    
                    -- ===== MOTIVOS DE CANCELAMENTOS E RECUSAS (FOCO PRINCIPAL) =====
                    motivo_recusa TEXT,
                    motivo_cancelamento TEXT,
                    motivo_nao_consultado TEXT,
                    motivo_nao_cancelado TEXT,
                    motivo_nao_aberto TEXT,
                    motivo_nao_reagendado TEXT,
                    
                    -- ===== INEFICIÊNCIAS =====
                    tipo_ineficiencia TEXT,  -- 'APROVISIONAMENTO', 'CPF INVALIDO', 'NUMERO VAGO'
                    
                    -- ===== BONUS PORTABILIDADE (BP) =====
                    status_bp_inicial TEXT,
                    status_bp_atual TEXT,
                    data_consulta_bp TEXT,
                    hora_consulta_bp TEXT,
                    
                    -- ===== LOGÍSTICA DETALHADA =====
                    rastreio TEXT,
                    cod_rastreio TEXT,  -- Link formatado
                    previsao_entrega TEXT,
                    prazo_entrega INTEGER,
                    data_entrega TEXT,
                    
                    -- ===== PRÉ-CRIVO =====
                    status_precrivo TEXT,
                    data_precrivo TEXT,
                    hora_precrivo TEXT,
                    
                    -- ===== DADOS ADICIONAIS =====
                    produto_vendido TEXT,
                    plano TEXT,
                    data_venda TEXT,
                    data_bruta TEXT,
                    hora_bruta TEXT,
                    data_gross TEXT,  -- Data de abertura do bilhete
                    chip_id TEXT,
                    
                    -- ===== TRIGGERS E REGRAS =====
                    regra_id INTEGER,
                    o_que_aconteceu TEXT,
                    acao_a_realizar TEXT,
                    tipo_mensagem TEXT,
                    template TEXT,
                    mapeado INTEGER DEFAULT 1,
                    
                    -- ===== CONTROLE DE VERSÃO =====
                    hash_dados TEXT,  -- Hash dos dados para detectar mudanças
                    is_latest INTEGER DEFAULT 1,  -- 1 = versão mais recente, 0 = histórico
                    
                    -- ===== METADADOS =====
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabela de mudanças detectadas (auditoria)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tim_unificado_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_isize TEXT NOT NULL,
                    versao INTEGER NOT NULL,
                    campo_alterado TEXT NOT NULL,
                    valor_anterior TEXT,
                    valor_novo TEXT,
                    data_mudanca TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    origem_mudanca TEXT,
                    FOREIGN KEY (id_isize, versao) REFERENCES tim_unificado(id_isize, versao)
                )
            """)
            
            # Tabela de sincronização (rastreamento de fontes)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tim_unificado_sync (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_isize TEXT NOT NULL,
                    fonte_dados TEXT NOT NULL,
                    data_sincronizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    registros_processados INTEGER,
                    registros_atualizados INTEGER,
                    registros_novos INTEGER,
                    status TEXT,  -- 'sucesso', 'erro', 'parcial'
                    observacoes TEXT
                )
            """)
            
            conn.commit()
            logger.info("Banco de dados unificado inicializado com sucesso")
    
    def _create_unified_indexes(self):
        """Cria índices otimizados para o banco unificado"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Índices principais
            indexes = [
                # Índices para busca por ID
                ("idx_unified_id_isize", "tim_unificado(id_isize, is_latest)"),
                ("idx_unified_numero_ordem", "tim_unificado(numero_ordem, is_latest)"),
                ("idx_unified_cpf", "tim_unificado(cpf, is_latest)"),
                ("idx_unified_codigo_externo", "tim_unificado(codigo_externo, is_latest)"),
                
                # Índices para campos de foco (status)
                ("idx_unified_status_ordem", "tim_unificado(status_ordem, is_latest)"),
                ("idx_unified_status_logistica", "tim_unificado(status_logistica, is_latest)"),
                ("idx_unified_status_bilhete", "tim_unificado(status_bilhete, is_latest)"),
                
                # Índices para versionamento
                ("idx_unified_version", "tim_unificado(id_isize, versao)"),
                ("idx_unified_latest", "tim_unificado(is_latest) WHERE is_latest = 1"),
                
                # Índices para datas
                ("idx_unified_data_armazenamento", "tim_unificado(data_armazenamento)"),
                ("idx_unified_data_venda", "tim_unificado(data_venda)"),
                
                # Índices para sincronização
                ("idx_sync_id_isize", "tim_unificado_sync(id_isize, data_sincronizacao)"),
                ("idx_changes_id_isize", "tim_unificado_changes(id_isize, versao)"),
            ]
            
            for idx_name, idx_def in indexes:
                try:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")
                except Exception as e:
                    logger.warning(f"Erro ao criar índice {idx_name}: {e}")
            
            conn.commit()
            logger.debug("Índices do banco unificado criados/verificados")
    
    @contextmanager
    def _get_connection(self):
        """Context manager para conexões com o banco"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro na transação: {e}")
            raise
        finally:
            conn.close()
    
    def _calculate_hash(self, data: Dict[str, Any]) -> str:
        """
        Calcula hash dos dados principais para detectar mudanças
        
        Args:
            data: Dicionário com os dados do registro
            
        Returns:
            Hash MD5 dos dados relevantes
        """
        # Campos que definem se houve mudança significativa
        relevant_fields = [
            'status_ordem', 'status_logistica', 'status_bilhete',
            'motivo_recusa', 'motivo_cancelamento',
            'data_portabilidade', 'data_entrega', 'data_logistica'
        ]
        
        relevant_data = {k: str(data.get(k, '')) for k in relevant_fields if k in data}
        data_str = json.dumps(relevant_data, sort_keys=True, ensure_ascii=False)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()
    
    def insert_or_update_record(
        self,
        id_isize: str,
        numero_ordem: str,
        dados: Dict[str, Any],
        origem_dados: str = 'gerenciador',
        forcar_nova_versao: bool = False
    ) -> Tuple[int, bool]:
        """
        Insere ou atualiza um registro com versionamento
        
        Se os dados mudaram ou forcar_nova_versao=True, cria uma nova versão.
        Caso contrário, atualiza a versão mais recente.
        
        Args:
            id_isize: ID único do iSize (mantém o mesmo para todas as versões)
            numero_ordem: Número da ordem (chave de rastreamento)
            dados: Dicionário com todos os dados do registro
            origem_dados: Origem dos dados ('base_analitica', 'relatorio_objetos', etc)
            forcar_nova_versao: Se True, sempre cria nova versão mesmo sem mudanças
        
        Returns:
            Tupla (versao, is_nova_versao) onde is_nova_versao indica se foi criada nova versão
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Calcular hash dos dados
            hash_dados = self._calculate_hash(dados)
            
            # Buscar versão mais recente
            cursor.execute("""
                SELECT registro_id, versao, hash_dados, is_latest
                FROM tim_unificado
                WHERE id_isize = ? AND is_latest = 1
                ORDER BY versao DESC
                LIMIT 1
            """, (id_isize,))
            
            existing = cursor.fetchone()
            
            if existing and not forcar_nova_versao:
                existing_hash = existing['hash_dados']
                existing_version = existing['versao']
                
                # Se os dados não mudaram, não cria nova versão
                if existing_hash == hash_dados:
                    logger.debug(f"Registro {id_isize} sem mudanças, mantendo versão {existing_version}")
                    return existing_version, False
            
            # Criar nova versão ou primeira versão
            if existing:
                nova_versao = existing['versao'] + 1
                # Marcar versão anterior como não mais recente
                cursor.execute("""
                    UPDATE tim_unificado
                    SET is_latest = 0
                    WHERE id_isize = ? AND versao = ?
                """, (id_isize, existing['versao']))
            else:
                nova_versao = 1
            
            # Preparar dados para inserção
            dados_insert = {
                'id_isize': id_isize,
                'versao': nova_versao,
                'numero_ordem': numero_ordem,
                'origem_dados': origem_dados,
                'hash_dados': hash_dados,
                'is_latest': 1,
                'data_armazenamento': datetime.now().isoformat(),
                **dados
            }
            
            # Se há versão anterior, copiar campos que não mudaram
            if existing:
                cursor.execute("""
                    SELECT * FROM tim_unificado
                    WHERE id_isize = ? AND versao = ?
                """, (id_isize, existing['versao']))
                prev_row = cursor.fetchone()
                previous_data = {key: prev_row[key] for key in prev_row.keys()}
                
                # Copiar campos que não foram fornecidos nos novos dados
                for key, value in previous_data.items():
                    if key not in dados_insert and key not in ['registro_id', 'versao', 'is_latest', 
                                                                'data_armazenamento', 'hash_dados', 'created_at']:
                        dados_insert[key] = value
                
                # Registrar campos que mudaram
                self._register_changes(cursor, id_isize, nova_versao, previous_data, dados_insert, origem_dados)
            
            # Inserir nova versão
            columns = ', '.join(dados_insert.keys())
            placeholders = ', '.join(['?' for _ in dados_insert])
            values = list(dados_insert.values())
            
            cursor.execute(f"""
                INSERT INTO tim_unificado ({columns})
                VALUES ({placeholders})
            """, values)
            
            logger.info(f"Registro {id_isize} versão {nova_versao} criado (origem: {origem_dados})")
            return nova_versao, True
    
    def _register_changes(
        self,
        cursor,
        id_isize: str,
        nova_versao: int,
        dados_anteriores: Dict[str, Any],
        dados_novos: Dict[str, Any],
        origem: str
    ):
        """
        Registra as mudanças detectadas entre versões
        """
        campos_interesse = [
            'status_ordem', 'status_logistica', 'status_bilhete',
            'motivo_recusa', 'motivo_cancelamento',
            'data_portabilidade', 'data_entrega', 'data_logistica'
        ]
        
        for campo in campos_interesse:
            valor_anterior = dados_anteriores.get(campo)
            valor_novo = dados_novos.get(campo)
            
            if valor_anterior != valor_novo and (valor_anterior is not None or valor_novo is not None):
                cursor.execute("""
                    INSERT INTO tim_unificado_changes 
                    (id_isize, versao, campo_alterado, valor_anterior, valor_novo, origem_mudanca)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    id_isize,
                    nova_versao,
                    campo,
                    str(valor_anterior) if valor_anterior is not None else None,
                    str(valor_novo) if valor_novo is not None else None,
                    origem
                ))
    
    def get_latest_record(self, id_isize: str) -> Optional[Dict[str, Any]]:
        """
        Busca a versão mais recente de um registro
        
        Args:
            id_isize: ID único do iSize
            
        Returns:
            Dicionário com os dados da versão mais recente ou None
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM tim_unificado
                WHERE id_isize = ? AND is_latest = 1
                ORDER BY versao DESC
                LIMIT 1
            """, (id_isize,))
            
            row = cursor.fetchone()
            if row:
                return {key: row[key] for key in row.keys()}
            return None
    
    def get_record_history(self, id_isize: str) -> List[Dict[str, Any]]:
        """
        Busca todo o histórico de versões de um registro
        
        Args:
            id_isize: ID único do iSize
            
        Returns:
            Lista de dicionários com todas as versões (ordenadas por versão)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM tim_unificado
                WHERE id_isize = ?
                ORDER BY versao ASC
            """, (id_isize,))
            
            return [{key: row[key] for key in row.keys()} for row in cursor.fetchall()]
    
    def get_records_by_status(
        self,
        status_ordem: Optional[str] = None,
        status_logistica: Optional[str] = None,
        status_bilhete: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Busca registros filtrando por status
        
        Args:
            status_ordem: Filtrar por status da ordem
            status_logistica: Filtrar por status logística
            status_bilhete: Filtrar por status do bilhete
            limit: Limite de resultados
            
        Returns:
            Lista de registros (apenas versões mais recentes)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            conditions = ["is_latest = 1"]
            params = []
            
            if status_ordem:
                conditions.append("status_ordem = ?")
                params.append(status_ordem)
            
            if status_logistica:
                conditions.append("status_logistica = ?")
                params.append(status_logistica)
            
            if status_bilhete:
                conditions.append("status_bilhete = ?")
                params.append(status_bilhete)
            
            query = f"""
                SELECT * FROM tim_unificado
                WHERE {' AND '.join(conditions)}
                ORDER BY data_armazenamento DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query, params)
            return [{key: row[key] for key in row.keys()} for row in cursor.fetchall()]

