"""
Loader para Relatório de Objetos (dados de logística)
Versão 2.0 - Integração otimizada com sistema de portabilidade
- Indexação otimizada com múltiplas chaves
- Cache para buscas repetidas
- Priorização de registros mais recentes
"""
import logging
import re
from pathlib import Path
from typing import Optional, Dict, Any, List, Set
from datetime import datetime
from dataclasses import dataclass
from functools import lru_cache

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ObjectRecord:
    """Registro de objeto (logística)"""
    nu_pedido: str
    codigo_externo: str  # Extraído do Nu Pedido
    id_erp: Optional[str] = None
    rastreio: Optional[str] = None
    destinatario: Optional[str] = None
    documento: Optional[str] = None
    telefone: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cep: Optional[str] = None
    data_criacao_pedido: Optional[datetime] = None
    data_insercao: Optional[datetime] = None
    status: Optional[str] = None
    transportadora: Optional[str] = None
    previsao_entrega: Optional[datetime] = None
    data_entrega: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """Converte para dicionário"""
        return {
            'nu_pedido': self.nu_pedido,
            'codigo_externo': self.codigo_externo,
            'id_erp': self.id_erp,
            'rastreio': self.rastreio,
            'destinatario': self.destinatario,
            'documento': self.documento,
            'telefone': self.telefone,
            'cidade': self.cidade,
            'uf': self.uf,
            'cep': self.cep,
            'data_criacao_pedido': self.data_criacao_pedido.isoformat() if self.data_criacao_pedido else None,
            'data_insercao': self.data_insercao.isoformat() if self.data_insercao else None,
            'status': self.status,
            'transportadora': self.transportadora,
            'previsao_entrega': self.previsao_entrega.isoformat() if self.previsao_entrega else None,
            'data_entrega': self.data_entrega.isoformat() if self.data_entrega else None,
        }


class ObjectsLoader:
    """
    Carrega e indexa dados do Relatório de Objetos (logística)
    
    O Relatório de Objetos contém informações de logística como:
    - Dados do destinatário (nome, telefone, endereço)
    - Rastreio e status de entrega
    - Datas de criação e inserção
    
    Versão 2.0 - Otimizações:
    - Múltiplos índices para busca rápida
    - Priorização automática de registros mais recentes
    - Cache LRU para buscas repetidas
    - Indexação por CPF para fallback
    """
    
    def __init__(self, file_path: Optional[str] = None):
        """
        Inicializa o loader
        
        Args:
            file_path: Caminho para o arquivo xlsx do Relatório de Objetos
        """
        self.file_path = file_path
        self._records: List[ObjectRecord] = []
        self._index_by_codigo: Dict[str, ObjectRecord] = {}
        self._index_by_erp: Dict[str, ObjectRecord] = {}
        self._index_by_cpf: Dict[str, List[ObjectRecord]] = {}  # Novo: índice por CPF
        self._index_by_nu_pedido: Dict[str, ObjectRecord] = {}  # Novo: índice por Nu Pedido original
        self._loaded = False
        self._search_cache: Dict[str, Optional[ObjectRecord]] = {}  # Cache de buscas
        
        if file_path:
            self.load(file_path)
    
    def load(self, file_path: str) -> int:
        """
        Carrega o Relatório de Objetos com indexação otimizada
        
        Args:
            file_path: Caminho para o arquivo xlsx
            
        Returns:
            Número de registros carregados
        """
        self.file_path = file_path
        
        if not Path(file_path).exists():
            logger.warning(f"Arquivo não encontrado: {file_path}")
            return 0
        
        try:
            logger.info(f"Carregando Relatório de Objetos: {file_path}")
            
            # Otimização: usar engine apropriado e tipos de dados
            df = pd.read_excel(file_path, engine='openpyxl')
            
            # Limpar caches e índices
            self._records = []
            self._index_by_codigo = {}
            self._index_by_erp = {}
            self._index_by_cpf = {}
            self._index_by_nu_pedido = {}
            self._search_cache = {}
            
            # Processar em batch para melhor performance
            records_to_process = []
            for _, row in df.iterrows():
                record = self._parse_row(row)
                if record:
                    records_to_process.append(record)
            
            # Ordenar por data de inserção (mais recente primeiro) para indexação eficiente
            records_to_process.sort(
                key=lambda x: x.data_insercao or datetime.min, 
                reverse=True
            )
            
            # Indexar (como já está ordenado, o primeiro de cada chave é o mais recente)
            for record in records_to_process:
                self._records.append(record)
                
                # Indexar por código externo (primeiro = mais recente)
                if record.codigo_externo and record.codigo_externo not in self._index_by_codigo:
                    self._index_by_codigo[record.codigo_externo] = record
                
                # Indexar por ID ERP
                if record.id_erp and record.id_erp not in self._index_by_erp:
                    self._index_by_erp[record.id_erp] = record
                
                # Indexar por Nu Pedido original
                if record.nu_pedido and record.nu_pedido not in self._index_by_nu_pedido:
                    self._index_by_nu_pedido[record.nu_pedido] = record
                
                # Indexar por CPF (lista para múltiplos registros)
                if record.documento:
                    if record.documento not in self._index_by_cpf:
                        self._index_by_cpf[record.documento] = []
                    self._index_by_cpf[record.documento].append(record)
            
            self._loaded = True
            logger.info(f"Carregados {len(self._records)} registros do Relatório de Objetos")
            logger.info(f"  - Índice por código externo: {len(self._index_by_codigo)} registros únicos")
            logger.info(f"  - Índice por ID ERP: {len(self._index_by_erp)} registros únicos")
            logger.info(f"  - Índice por CPF: {len(self._index_by_cpf)} CPFs únicos")
            logger.info(f"  - Índice por Nu Pedido: {len(self._index_by_nu_pedido)} pedidos únicos")
            
            return len(self._records)
            
        except Exception as e:
            logger.error(f"Erro ao carregar Relatório de Objetos: {e}")
            return 0
    
    def _parse_row(self, row: pd.Series) -> Optional[ObjectRecord]:
        """
        Parse de uma linha do DataFrame
        
        Args:
            row: Linha do DataFrame
            
        Returns:
            ObjectRecord ou None
        """
        try:
            nu_pedido = self._clean_value(row.get('Nu Pedido'))
            if not nu_pedido:
                return None
            
            # Extrair código externo do Nu Pedido
            # Formato: "26-0250015976" -> "250015976"
            codigo_externo = self._extract_codigo_externo(nu_pedido)
            
            return ObjectRecord(
                nu_pedido=nu_pedido,
                codigo_externo=codigo_externo,
                id_erp=self._clean_value(row.get('ID ERP')),
                rastreio=self._clean_value(row.get('Rastreio')),
                destinatario=self._clean_value(row.get('Destinatário')),
                documento=self._clean_cpf(row.get('Documento')),
                telefone=self._clean_phone(row.get('Telefone')),
                cidade=self._clean_value(row.get('Cidade')),
                uf=self._clean_value(row.get('UF')),
                cep=self._clean_value(row.get('CEP')),
                data_criacao_pedido=self._parse_date(row.get('Data Criação Pedido')),
                data_insercao=self._parse_date(row.get('Data Inserção')),
                status=self._clean_value(row.get('Status')),
                transportadora=self._clean_value(row.get('Transportadora')),
                previsao_entrega=self._parse_date(row.get('Previsão Entrega')),
                data_entrega=self._parse_date(row.get('Data Entrega')),
            )
            
        except Exception as e:
            logger.debug(f"Erro ao parsear linha: {e}")
            return None
    
    def _extract_codigo_externo(self, nu_pedido: str) -> str:
        """
        Extrai código externo do Nu Pedido
        
        Formatos suportados:
        - "26-0250015976" -> "250015976"
        - "26-0250015976-01" -> "250015976"
        - "250015976" -> "250015976"
        
        Args:
            nu_pedido: Número do pedido
            
        Returns:
            Código externo
        """
        if not nu_pedido:
            return ""
        
        # Remover sufixos como "-01", "-02"
        base = nu_pedido.split('-')[0] if '-' in nu_pedido else nu_pedido
        
        # Se ainda tem hífen, pegar segunda parte
        if '-' in nu_pedido:
            parts = nu_pedido.split('-')
            if len(parts) >= 2:
                # Formato "26-0250015976" ou "26-0250015976-01"
                base = parts[1]
                # Remover prefixo "0" se existir
                if base.startswith('0'):
                    base = base[1:]
        
        # Garantir que é numérico
        base = re.sub(r'[^0-9]', '', base)
        
        return base
    
    def _is_more_recent(self, new_record: ObjectRecord, existing: ObjectRecord) -> bool:
        """
        Verifica se o novo registro é mais recente que o existente
        
        Args:
            new_record: Novo registro
            existing: Registro existente
            
        Returns:
            True se novo é mais recente
        """
        if not new_record.data_insercao:
            return False
        if not existing.data_insercao:
            return True
        return new_record.data_insercao > existing.data_insercao
    
    def find_by_codigo_externo(self, codigo: str) -> Optional[ObjectRecord]:
        """
        Busca registro por código externo (com cache)
        
        Args:
            codigo: Código externo (ex: "250015976")
            
        Returns:
            ObjectRecord ou None
        """
        if not codigo:
            return None
        
        # Verificar cache
        cache_key = f"codigo:{codigo}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]
        
        # Limpar código (remover zeros à esquerda e caracteres não numéricos)
        codigo_limpo = re.sub(r'[^0-9]', '', str(codigo)).lstrip('0')
        
        # Tentar busca direta
        record = self._index_by_codigo.get(codigo_limpo)
        if record:
            self._search_cache[cache_key] = record
            return record
        
        # Tentar com zeros à esquerda (variações comuns)
        for i in range(1, 4):
            codigo_com_zeros = codigo_limpo.zfill(len(codigo_limpo) + i)
            record = self._index_by_codigo.get(codigo_com_zeros)
            if record:
                self._search_cache[cache_key] = record
                return record
        
        # Tentar busca no índice de Nu Pedido (formato completo como 26-0250015976)
        for nu_pedido, rec in self._index_by_nu_pedido.items():
            if codigo_limpo in nu_pedido or codigo in nu_pedido:
                self._search_cache[cache_key] = rec
                return rec
        
        self._search_cache[cache_key] = None
        return None
    
    def find_by_id_erp(self, id_erp: str) -> Optional[ObjectRecord]:
        """
        Busca registro por ID ERP (número da ordem)
        
        Args:
            id_erp: ID ERP (ex: "1-1701687349481")
            
        Returns:
            ObjectRecord ou None
        """
        if not id_erp:
            return None
        return self._index_by_erp.get(str(id_erp))
    
    def find_by_cpf(self, cpf: str) -> Optional[ObjectRecord]:
        """
        Busca registro por CPF (busca no índice por CPF)
        Retorna o mais recente se houver múltiplos
        
        Args:
            cpf: CPF do cliente
            
        Returns:
            ObjectRecord ou None
        """
        if not cpf:
            return None
        
        # Verificar cache
        cache_key = f"cpf:{cpf}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]
        
        cpf_limpo = re.sub(r'[^0-9]', '', str(cpf))
        
        # Usar índice por CPF (já ordenado por data, primeiro é mais recente)
        matches = self._index_by_cpf.get(cpf_limpo, [])
        
        if not matches:
            self._search_cache[cache_key] = None
            return None
        
        # O primeiro já é o mais recente (pré-ordenado durante load)
        result = matches[0]
        self._search_cache[cache_key] = result
        return result
    
    def find_best_match(self, codigo_externo: str = None, id_erp: str = None, cpf: str = None) -> Optional[ObjectRecord]:
        """
        Busca o melhor match usando múltiplas chaves (com cache combinado)
        Prioridade: código_externo > id_erp > cpf
        
        Args:
            codigo_externo: Código externo
            id_erp: ID ERP / Número da ordem
            cpf: CPF do cliente
            
        Returns:
            ObjectRecord ou None (sempre o mais recente disponível)
        """
        # Cache combinado para evitar buscas repetidas
        cache_key = f"best:{codigo_externo}:{id_erp}:{cpf}"
        if cache_key in self._search_cache:
            return self._search_cache[cache_key]
        
        result = None
        
        # Tentar por código externo primeiro (mais específico)
        if codigo_externo:
            result = self.find_by_codigo_externo(codigo_externo)
            if result:
                self._search_cache[cache_key] = result
                return result
        
        # Tentar por ID ERP
        if id_erp:
            result = self.find_by_id_erp(id_erp)
            if result:
                self._search_cache[cache_key] = result
                return result
        
        # Tentar por CPF (fallback)
        if cpf:
            result = self.find_by_cpf(cpf)
            if result:
                self._search_cache[cache_key] = result
                return result
        
        self._search_cache[cache_key] = None
        return None
    
    def find_by_nu_pedido(self, nu_pedido: str) -> Optional[ObjectRecord]:
        """
        Busca registro pelo número do pedido original (Nu Pedido)
        
        Args:
            nu_pedido: Número do pedido no formato original (ex: "26-0250015976")
            
        Returns:
            ObjectRecord ou None
        """
        if not nu_pedido:
            return None
        
        return self._index_by_nu_pedido.get(str(nu_pedido).strip())
    
    def clear_cache(self):
        """Limpa o cache de buscas"""
        self._search_cache = {}
    
    @staticmethod
    def _clean_value(value) -> Optional[str]:
        """Limpa valor removendo NaN e espaços"""
        if value is None:
            return None
        if isinstance(value, float):
            import math
            if math.isnan(value):
                return None
        value_str = str(value).strip()
        if value_str.lower() in ['nan', 'none', '']:
            return None
        return value_str
    
    @staticmethod
    def _clean_cpf(value) -> Optional[str]:
        """Limpa CPF mantendo apenas dígitos"""
        cleaned = ObjectsLoader._clean_value(value)
        if not cleaned:
            return None
        return re.sub(r'[^0-9]', '', cleaned)
    
    @staticmethod
    def _clean_phone(value) -> Optional[str]:
        """Limpa telefone mantendo apenas dígitos"""
        cleaned = ObjectsLoader._clean_value(value)
        if not cleaned:
            return None
        return re.sub(r'[^0-9]', '', cleaned)
    
    @staticmethod
    def _parse_date(value) -> Optional[datetime]:
        """Parse de data com múltiplos formatos"""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        
        value_str = str(value).strip()
        if not value_str or value_str.lower() in ['nan', 'none', 'nat']:
            return None
        
        formats = [
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(value_str, fmt)
            except ValueError:
                continue
        
        return None
    
    @property
    def is_loaded(self) -> bool:
        """Retorna se os dados foram carregados"""
        return self._loaded
    
    @property
    def total_records(self) -> int:
        """Retorna total de registros"""
        return len(self._records)
    
    def get_stats(self) -> dict:
        """Retorna estatísticas detalhadas do loader"""
        return {
            'loaded': self._loaded,
            'total_records': len(self._records),
            'unique_by_codigo': len(self._index_by_codigo),
            'unique_by_erp': len(self._index_by_erp),
            'unique_by_cpf': len(self._index_by_cpf),
            'unique_by_nu_pedido': len(self._index_by_nu_pedido),
            'cache_size': len(self._search_cache),
            'file_path': self.file_path,
        }
