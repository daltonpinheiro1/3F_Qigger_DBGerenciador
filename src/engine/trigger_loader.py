"""
TriggerLoader - Carrega e gerencia regras do triggers.xlsx
Versão 2.0 - Com cache otimizado e early returns
"""
import logging
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import hashlib

from src.models.portabilidade import TriggerRule, PortabilidadeRecord

logger = logging.getLogger(__name__)


class TriggerLoader:
    """
    Carrega e gerencia regras de triggers do arquivo xlsx.
    Implementa cache em memória, indexação por chaves e sincronização com banco de dados.
    
    Versão 2.0 - Otimizações:
    - Índice por status_bilhete para busca O(1)
    - Cache de matching por chave composta
    - Early returns no algoritmo de matching
    """
    
    def __init__(self, xlsx_path: str = "triggers.xlsx"):
        """
        Inicializa o TriggerLoader
        
        Args:
            xlsx_path: Caminho para o arquivo triggers.xlsx
        """
        self.xlsx_path = Path(xlsx_path)
        self._rules_cache: List[TriggerRule] = []
        self._rules_loaded = False
        self._last_load_time: Optional[datetime] = None
        
        # Novos índices para busca otimizada
        self._index_by_status: Dict[str, List[TriggerRule]] = {}  # Índice por status_bilhete
        self._index_by_regra_id: Dict[int, TriggerRule] = {}  # Índice por regra_id
        self._matching_cache: Dict[str, Optional[TriggerRule]] = {}  # Cache de matching
    
    def load_rules(self, force_reload: bool = False) -> List[TriggerRule]:
        """
        Carrega regras do arquivo xlsx com indexação otimizada
        
        Args:
            force_reload: Se True, força recarregamento mesmo se já carregado
            
        Returns:
            Lista de regras carregadas
        """
        if self._rules_loaded and not force_reload:
            return self._rules_cache
        
        if not self.xlsx_path.exists():
            logger.error(f"Arquivo de triggers não encontrado: {self.xlsx_path}")
            raise FileNotFoundError(f"Arquivo de triggers não encontrado: {self.xlsx_path}")
        
        try:
            df = pd.read_excel(self.xlsx_path, engine='openpyxl')
            
            # Limpar caches e índices
            self._rules_cache = []
            self._index_by_status = {}
            self._index_by_regra_id = {}
            self._matching_cache = {}
            
            for _, row in df.iterrows():
                try:
                    rule = TriggerRule.from_dict(row.to_dict())
                    self._rules_cache.append(rule)
                    
                    # Indexar por regra_id
                    if rule.regra_id:
                        self._index_by_regra_id[rule.regra_id] = rule
                    
                    # Indexar por status_bilhete para busca O(1)
                    status_key = str(rule.status_bilhete).strip() if rule.status_bilhete else '__NONE__'
                    if status_key not in self._index_by_status:
                        self._index_by_status[status_key] = []
                    self._index_by_status[status_key].append(rule)
                    
                except Exception as e:
                    logger.warning(f"Erro ao parsear regra: {e}")
                    continue
            
            self._rules_loaded = True
            self._last_load_time = datetime.now()
            logger.info(f"Carregadas {len(self._rules_cache)} regras de {self.xlsx_path}")
            logger.info(f"  - Índice por status: {len(self._index_by_status)} status únicos")
            
            return self._rules_cache
            
        except Exception as e:
            logger.error(f"Erro ao carregar triggers.xlsx: {e}")
            raise
    
    def _generate_cache_key(self, keys: Dict[str, Any]) -> str:
        """Gera uma chave única para cache baseada nas chaves de matching"""
        key_str = "|".join([
            str(keys.get('status_bilhete', '')),
            str(keys.get('operadora_doadora', '')),
            str(keys.get('motivo_recusa', '')),
            str(keys.get('motivo_cancelamento', '')),
            str(keys.get('ultimo_bilhete', '')),
            str(keys.get('motivo_nao_consultado', '')),
        ])
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def find_matching_rule(self, record: PortabilidadeRecord) -> Optional[TriggerRule]:
        """
        Encontra a regra que corresponde ao registro (otimizado com índices e cache)
        
        Args:
            record: Registro de portabilidade
            
        Returns:
            Regra correspondente ou None se não encontrada
        """
        if not self._rules_loaded:
            self.load_rules()
        
        matching_keys = record.get_matching_keys()
        
        # Verificar cache primeiro
        cache_key = self._generate_cache_key(matching_keys)
        if cache_key in self._matching_cache:
            return self._matching_cache[cache_key]
        
        # Busca otimizada: primeiro filtrar por status_bilhete usando índice
        status_bilhete = matching_keys.get('status_bilhete')
        status_key = str(status_bilhete).strip() if status_bilhete else '__NONE__'
        
        # Candidatas: regras com mesmo status OU regras sem status (wildcards)
        candidate_rules = []
        
        # Regras com status específico
        if status_key in self._index_by_status:
            candidate_rules.extend(self._index_by_status[status_key])
        
        # Regras sem status (aplicam a qualquer status)
        if '__NONE__' in self._index_by_status and status_key != '__NONE__':
            candidate_rules.extend(self._index_by_status['__NONE__'])
        
        # Se não há candidatas, usar todas as regras (fallback)
        if not candidate_rules:
            candidate_rules = self._rules_cache
        
        # Busca por correspondência nas candidatas
        for rule in candidate_rules:
            if self._rule_matches(rule, matching_keys):
                self._matching_cache[cache_key] = rule
                return rule
        
        self._matching_cache[cache_key] = None
        return None
    
    def _rule_matches(self, rule: TriggerRule, keys: Dict[str, Any]) -> bool:
        """
        Verifica se uma regra corresponde às chaves fornecidas (com early returns)
        
        A lógica de matching é:
        - Se o campo da regra é None/vazio/NaN, ele corresponde a qualquer valor
        - Se o campo da regra tem valor, deve corresponder exatamente
        - Early returns para performance
        
        Args:
            rule: Regra a verificar
            keys: Dicionário com chaves do registro
            
        Returns:
            True se a regra corresponde
        """
        # Early return 1: Status do bilhete (campo mais discriminatório)
        if self._has_value(rule.status_bilhete):
            if not self._values_match(rule.status_bilhete, keys.get('status_bilhete')):
                return False
        
        # Early return 2: Operadora doadora
        rule_op = rule.operadora_doadora
        if self._has_value(rule_op):
            record_op = keys.get('operadora_doadora')
            if not self._values_match(rule_op, record_op):
                return False
        
        # Early return 3: Motivo da recusa
        rule_rec = rule.motivo_recusa
        if self._has_value(rule_rec):
            if not self._values_match(rule_rec, keys.get('motivo_recusa')):
                return False
        
        # Early return 4: Motivo do cancelamento
        rule_canc = rule.motivo_cancelamento
        if self._has_value(rule_canc):
            if not self._values_match(rule_canc, keys.get('motivo_cancelamento')):
                return False
        
        # Early return 5: Último bilhete - match exato quando especificado
        rule_ultimo = rule.ultimo_bilhete
        if rule_ultimo is not None:
            record_ultimo = keys.get('ultimo_bilhete')
            if record_ultimo is not None and record_ultimo != rule_ultimo:
                return False
        
        # Early return 6: Motivo de não ter sido consultado (parcial)
        rule_motivo = rule.motivo_nao_consultado
        if self._has_value(rule_motivo):
            record_motivo = keys.get('motivo_nao_consultado')
            if not self._partial_match(rule_motivo, record_motivo):
                return False
        
        return True
    
    @staticmethod
    def _has_value(value) -> bool:
        """Verifica se um valor é válido (não é None, NaN ou vazio)"""
        if value is None:
            return False
        if isinstance(value, float):
            import math
            if math.isnan(value):
                return False
        if isinstance(value, str) and value.strip() == '':
            return False
        return True
    
    @staticmethod
    def _values_match(rule_value, record_value) -> bool:
        """Compara dois valores para matching"""
        if rule_value is None or record_value is None:
            return rule_value == record_value
        
        # Converter para string para comparação segura
        rule_str = str(rule_value).strip() if rule_value else ''
        record_str = str(record_value).strip() if record_value else ''
        
        return rule_str == record_str
    
    @staticmethod
    def _partial_match(rule_value, record_value) -> bool:
        """Verifica match parcial (para textos longos)"""
        if not record_value:
            return False
        
        rule_str = str(rule_value).strip() if rule_value else ''
        record_str = str(record_value).strip() if record_value else ''
        
        # Match exato ou parcial
        return rule_str == record_str or rule_str in record_str or record_str in rule_str
    
    def add_unmapped_rule(self, record: PortabilidadeRecord) -> int:
        """
        Adiciona uma nova regra não mapeada ao arquivo xlsx
        
        Args:
            record: Registro que não foi mapeado
            
        Returns:
            ID da nova regra
        """
        try:
            # Carregar arquivo existente
            if self.xlsx_path.exists():
                df = pd.read_excel(self.xlsx_path)
            else:
                df = pd.DataFrame()
            
            # Determinar próximo ID
            if 'REGRA_ID' in df.columns and len(df) > 0:
                next_id = int(df['REGRA_ID'].max()) + 1
            else:
                next_id = 1
            
            # Criar nova linha com dados do registro
            matching_keys = record.get_matching_keys()
            new_row = {
                'REGRA_ID': next_id,
                'Status do bilhete': matching_keys.get('status_bilhete'),
                'Operadora doadora': matching_keys.get('operadora_doadora'),
                'Motivo da recusa': matching_keys.get('motivo_recusa'),
                'Motivo do cancelamento': matching_keys.get('motivo_cancelamento'),
                'Último bilhete de portabilidade?': 'Sim' if matching_keys.get('ultimo_bilhete') else 'Não',
                'Motivo de não ter sido consultado': matching_keys.get('motivo_nao_consultado'),
                'Novo status do bilhete': None,
                'Ajustes número de acesso': None,
                'O que aconteceu': 'NÃO MAPEADO - REVISAR',
                'Ação a ser realizada': 'DEFINIR AÇÃO',
                'Tipo de mensagem': 'PENDENTE',
                'Templete': None,  # Mantendo o typo do arquivo original
            }
            
            # Adicionar ao DataFrame
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            
            # Salvar arquivo
            df.to_excel(self.xlsx_path, index=False)
            
            # Atualizar cache
            new_rule = TriggerRule.from_dict(new_row)
            self._rules_cache.append(new_rule)
            
            logger.info(f"Nova regra não mapeada adicionada ao triggers.xlsx: ID {next_id}")
            return next_id
            
        except Exception as e:
            logger.error(f"Erro ao adicionar regra não mapeada: {e}")
            raise
    
    def get_all_rules(self) -> List[TriggerRule]:
        """Retorna todas as regras carregadas"""
        if not self._rules_loaded:
            self.load_rules()
        return self._rules_cache.copy()
    
    def get_rule_by_id(self, regra_id: int) -> Optional[TriggerRule]:
        """
        Busca uma regra pelo ID (usando índice O(1))
        
        Args:
            regra_id: ID da regra
            
        Returns:
            Regra encontrada ou None
        """
        if not self._rules_loaded:
            self.load_rules()
        
        return self._index_by_regra_id.get(regra_id)
    
    def clear_cache(self):
        """Limpa o cache de matching"""
        self._matching_cache = {}
    
    def get_rules_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas das regras carregadas
        
        Returns:
            Dicionário com estatísticas detalhadas
        """
        if not self._rules_loaded:
            self.load_rules()
        
        stats = {
            'total_regras': len(self._rules_cache),
            'ultimo_carregamento': self._last_load_time.isoformat() if self._last_load_time else None,
            'arquivo': str(self.xlsx_path),
            'por_tipo_mensagem': {},
            'por_acao': {},
            'por_status': {},
            'cache_size': len(self._matching_cache),
            'index_status_count': len(self._index_by_status),
        }
        
        for rule in self._rules_cache:
            # Contagem por tipo de mensagem
            tipo = rule.tipo_mensagem or 'SEM TIPO'
            stats['por_tipo_mensagem'][tipo] = stats['por_tipo_mensagem'].get(tipo, 0) + 1
            
            # Contagem por ação
            acao = rule.acao_a_realizar or 'SEM AÇÃO'
            stats['por_acao'][acao] = stats['por_acao'].get(acao, 0) + 1
            
            # Contagem por status
            status = rule.status_bilhete or 'SEM STATUS'
            stats['por_status'][status] = stats['por_status'].get(status, 0) + 1
        
        return stats
    
    def reload_if_modified(self) -> bool:
        """
        Recarrega as regras se o arquivo foi modificado
        
        Returns:
            True se recarregou, False caso contrário
        """
        if not self.xlsx_path.exists():
            return False
        
        file_mtime = datetime.fromtimestamp(self.xlsx_path.stat().st_mtime)
        
        if self._last_load_time is None or file_mtime > self._last_load_time:
            self.load_rules(force_reload=True)
            return True
        
        return False
    
    def get_rules_by_status(self, status_bilhete: str) -> List[TriggerRule]:
        """
        Retorna todas as regras para um status específico
        
        Args:
            status_bilhete: Status do bilhete
            
        Returns:
            Lista de regras
        """
        if not self._rules_loaded:
            self.load_rules()
        
        status_key = str(status_bilhete).strip() if status_bilhete else '__NONE__'
        return self._index_by_status.get(status_key, [])
