"""
Parser para arquivos CSV de importação do Siebel
Versão 2.0 - Adaptado para nova estrutura com triggers.xlsx
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
        Parse de arquivo CSV completo com detecção automática de encoding
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Lista de registros de portabilidade
        """
        records = []
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        # Tentar diferentes encodings comuns
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        encoding_usado = None
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.read()
                    encoding_usado = encoding
                    logger.debug(f"Arquivo {file_path} lido com encoding: {encoding}")
                    break
            except (UnicodeDecodeError, LookupError):
                continue
        
        if file_content is None:
            raise ValueError(
                f"Erro ao ler arquivo {file_path}: nenhum encoding funcionou. "
                f"Tentados: {', '.join(encodings)}"
            )
        
        # Parse do CSV
        import io
        f = io.StringIO(file_content)
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):
            try:
                record = cls._parse_row(row)
                if record:
                    records.append(record)
            except Exception as e:
                logger.error(f"Erro ao processar linha {row_num}: {e}")
                continue
        
        logger.info(f"Parseados {len(records)} registros do arquivo {file_path} (encoding: {encoding_usado})")
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
            
            # Se número da ordem estiver vazio, usar código externo como fallback
            if not numero_ordem and codigo_externo:
                numero_ordem = codigo_externo
            
            # Campos mínimos obrigatórios: CPF, número de acesso, código externo
            if not all([cpf, numero_acesso, codigo_externo]):
                logger.debug("Linha com campos obrigatórios ausentes (CPF, número de acesso ou código externo), pulando...")
                return None
            
            # Criar registro com a nova estrutura simplificada
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
                
                # Motivos (campos chave para matching com triggers)
                motivo_recusa=row.get('Motivo da recusa', '').strip() or None,
                motivo_cancelamento=row.get('Motivo do cancelamento', '').strip() or None,
                ultimo_bilhete=cls.parse_bool(row.get('Último bilhete de portabilidade?')),
                
                # Status da ordem
                status_ordem=cls.parse_status_ordem(row.get('Status da ordem')),
                preco_ordem=row.get('Preço da ordem', '').strip() or None,
                data_conclusao_ordem=cls.parse_date(row.get('Data da conclusão da ordem')),
                
                # Motivo de não consulta (campo chave para matching)
                motivo_nao_consultado=row.get('Motivo de não ter sido consultado', '').strip() or None,
                
                # Processamento
                responsavel_processamento=row.get('Responsável pelo processamento', '').strip() or None,
                data_inicial_processamento=cls.parse_date(row.get('Data inicial do processamento')),
                data_final_processamento=cls.parse_date(row.get('Data final do processamento')),
                
                # Validação básica
                registro_valido=cls.parse_bool(row.get('Registro válido?')),
            )
            
            return record
            
        except Exception as e:
            logger.error(f"Erro ao parsear linha: {e}")
            return None
    
    @classmethod
    def get_csv_headers(cls) -> List[str]:
        """
        Retorna os headers esperados do CSV
        
        Returns:
            Lista de headers
        """
        return [
            'Cpf',
            'Número de acesso',
            'Número da ordem',
            'Código externo',
            'Número temporário',
            'Bilhete temporário',
            'Número do bilhete',
            'Status do bilhete',
            'Operadora doadora',
            'Data da portabilidade',
            'Motivo da recusa',
            'Motivo do cancelamento',
            'Último bilhete de portabilidade?',
            'Status da ordem',
            'Preço da ordem',
            'Data da conclusão da ordem',
            'Motivo de não ter sido consultado',
            'Responsável pelo processamento',
            'Data inicial do processamento',
            'Data final do processamento',
            'Registro válido?',
        ]
    
    @classmethod
    def validate_csv_structure(cls, file_path: str) -> tuple[bool, List[str]]:
        """
        Valida a estrutura do arquivo CSV
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Tupla (válido, lista de erros)
        """
        errors = []
        
        if not Path(file_path).exists():
            return False, [f"Arquivo não encontrado: {file_path}"]
        
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                if not headers:
                    return False, ["Arquivo CSV vazio ou sem headers"]
                
                # Verificar campos obrigatórios
                required_fields = ['Cpf', 'Número de acesso', 'Número da ordem', 'Código externo']
                missing = [f for f in required_fields if f not in headers]
                
                if missing:
                    errors.append(f"Campos obrigatórios ausentes: {', '.join(missing)}")
                
                # Contar registros
                record_count = sum(1 for _ in reader)
                
                if record_count == 0:
                    errors.append("Arquivo não contém registros de dados")
                
        except Exception as e:
            errors.append(f"Erro ao ler arquivo: {e}")
        
        return len(errors) == 0, errors
