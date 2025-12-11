"""
Parser para arquivos CSV de importação do Siebel
"""
import csv
import logging
from datetime import datetime
from typing import List, Optional
from pathlib import Path

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem

logger = logging.getLogger(__name__)


class CSVParser:
    """Parser para arquivos CSV de portabilidade"""
    
    DATE_FORMATS = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]
    
    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime]:
        """Parse de data com múltiplos formatos"""
        if not date_str or date_str.strip() == "":
            return None
        
        for fmt in CSVParser.DATE_FORMATS:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        logger.warning(f"Formato de data não reconhecido: {date_str}")
        return None
    
    @staticmethod
    def parse_bool(value: Optional[str]) -> Optional[bool]:
        """Parse de valor booleano"""
        if not value:
            return None
        
        value_lower = value.strip().lower()
        if value_lower in ['sim', 'yes', 'true', '1', 's']:
            return True
        elif value_lower in ['não', 'nao', 'no', 'false', '0', 'n']:
            return False
        return None
    
    @staticmethod
    def parse_status_bilhete(status_str: Optional[str]) -> Optional[PortabilidadeStatus]:
        """Parse do status do bilhete"""
        if not status_str:
            return None
        
        status_str = status_str.strip()
        for status in PortabilidadeStatus:
            if status.value == status_str:
                return status
        return None
    
    @staticmethod
    def parse_status_ordem(status_str: Optional[str]) -> Optional[StatusOrdem]:
        """Parse do status da ordem"""
        if not status_str:
            return None
        
        status_str = status_str.strip()
        for status in StatusOrdem:
            if status.value == status_str:
                return status
        return None
    
    @classmethod
    def parse_file(cls, file_path: str) -> List[PortabilidadeRecord]:
        """
        Parse de arquivo CSV completo
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Lista de registros de portabilidade
        """
        records = []
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, start=2):  # Começa em 2 (linha 1 é header)
                try:
                    record = cls._parse_row(row)
                    if record:
                        records.append(record)
                except Exception as e:
                    logger.error(f"Erro ao processar linha {row_num}: {e}")
                    continue
        
        logger.info(f"Parseados {len(records)} registros do arquivo {file_path}")
        return records
    
    @classmethod
    def _parse_row(cls, row: dict) -> Optional[PortabilidadeRecord]:
        """Parse de uma linha do CSV"""
        try:
            # Campos obrigatórios
            cpf = row.get('Cpf', '').strip()
            numero_acesso = row.get('Número de acesso', '').strip()
            numero_ordem = row.get('Número da ordem', '').strip()
            codigo_externo = row.get('Código externo', '').strip()
            
            if not all([cpf, numero_acesso, numero_ordem, codigo_externo]):
                logger.warning("Linha com campos obrigatórios ausentes, pulando...")
                return None
            
            # Criar registro
            record = PortabilidadeRecord(
                cpf=cpf,
                numero_acesso=numero_acesso,
                numero_ordem=numero_ordem,
                codigo_externo=codigo_externo,
                
                # Bilhetes
                numero_temporario=row.get('Número temporário', '').strip() or None,
                bilhete_temporario=row.get('Bilhete temporário', '').strip() or None,
                numero_bilhete=row.get('Número do bilhete', '').strip() or None,
                status_bilhete=cls.parse_status_bilhete(row.get('Status do bilhete')),
                
                # Operadora e datas
                operadora_doadora=row.get('Operadora doadora', '').strip() or None,
                data_portabilidade=cls.parse_date(row.get('Data da portabilidade')),
                
                # Motivos
                motivo_recusa=row.get('Motivo da recusa', '').strip() or None,
                motivo_cancelamento=row.get('Motivo do cancelamento', '').strip() or None,
                ultimo_bilhete=cls.parse_bool(row.get('Último bilhete de portabilidade?')),
                
                # Status da ordem
                status_ordem=cls.parse_status_ordem(row.get('Status da ordem')),
                preco_ordem=row.get('Preço da ordem', '').strip() or None,
                data_conclusao_ordem=cls.parse_date(row.get('Data da conclusão da ordem')),
                
                # Motivos de não ação
                motivo_nao_consultado=row.get('Motivo de não ter sido consultado', '').strip() or None,
                motivo_nao_cancelado=row.get('Motivo de não ter sido cancelado', '').strip() or None,
                motivo_nao_aberto=row.get('Motivo de não ter sido aberto', '').strip() or None,
                motivo_nao_reagendado=row.get('Motivo de não ter sido reagendado', '').strip() or None,
                
                # Novos status
                novo_status_bilhete=cls.parse_status_bilhete(row.get('Novo status do bilhete')),
                nova_data_portabilidade=cls.parse_date(row.get('Nova data da portabilidade')),
                
                # Processamento
                responsavel_processamento=row.get('Responsável pelo processamento', '').strip() or None,
                data_inicial_processamento=cls.parse_date(row.get('Data inicial do processamento')),
                data_final_processamento=cls.parse_date(row.get('Data final do processamento')),
                
                # Validações
                registro_valido=cls.parse_bool(row.get('Registro válido?')),
                ajustes_registro=row.get('Ajustes registro', '').strip() or None,
                numero_acesso_valido=cls.parse_bool(row.get('Número de acesso válido?')),
                ajustes_numero_acesso=row.get('Ajustes número de acesso', '').strip() or None,
            )
            
            return record
            
        except Exception as e:
            logger.error(f"Erro ao parsear linha: {e}")
            return None

