"""
Parser para arquivos CSV de importação do Siebel
Versão 2.0 - Adaptado para nova estrutura com triggers.xlsx
Versão 2.1 - Suporte a cabeçalhos flexíveis (normalização por acentos/case)
"""
import csv
import logging
import unicodedata
from datetime import datetime
from typing import List, Optional, Dict, Tuple
from pathlib import Path

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem

logger = logging.getLogger(__name__)

# Mapeamento flexível: chave normalizada -> coluna esperada
# Permite aceitar variações de título (CPF, cpf, Cpf, etc.)
_CAMPO_CSV_ESPERADOS = [
    ('Cpf', ['cpf', 'documento', 'cpf cliente', 'documento cliente']),
    ('Número de acesso', ['numero de acesso', 'numero acesso', 'numero_acesso', 'num acesso']),
    ('Número da ordem', ['numero da ordem', 'numero ordem', 'numero_ordem', 'num ordem', 'ordem', 'id ordem']),
    ('Código externo', ['codigo externo', 'codigo_externo', 'cod externo', 'login externo']),
    ('Número temporário', ['numero temporario', 'numero_temporario']),
    ('Bilhete temporário', ['bilhete temporario', 'bilhete_temporario']),
    ('Número do bilhete', ['numero do bilhete', 'numero bilhete', 'numero_bilhete']),
    ('Status do bilhete', ['status do bilhete', 'status bilhete', 'status_bilhete']),
    ('Operadora doadora', ['operadora doadora', 'operadora_doadora']),
    ('Data da portabilidade', ['data da portabilidade', 'data portabilidade', 'data_portabilidade']),
    ('Motivo da recusa', ['motivo da recusa', 'motivo recusa', 'motivo_recusa']),
    ('Motivo do cancelamento', ['motivo do cancelamento', 'motivo cancelamento', 'motivo_cancelamento']),
    ('Último bilhete de portabilidade?', ['ultimo bilhete', 'ultimo bilhete portabilidade']),
    ('Status da ordem', ['status da ordem', 'status ordem', 'status_ordem']),
    ('Preço da ordem', ['preco da ordem', 'preco ordem', 'preco_ordem']),
    ('Data da conclusão da ordem', ['data conclusao ordem', 'data conclusao_ordem']),
    ('Motivo de não ter sido consultado', ['motivo nao consultado', 'motivo nao_consultado']),
    ('Responsável pelo processamento', ['responsavel processamento', 'responsavel_processamento']),
    ('Data inicial do processamento', ['data inicial processamento']),
    ('Data final do processamento', ['data final processamento']),
    ('Registro válido?', ['registro valido', 'registro_valido']),
]


class CSVParser:
    """Parser para arquivos CSV de portabilidade"""
    
    DATE_FORMATS = [
        # Formatos brasileiros (mais comuns primeiro)
        "%d/%m/%Y %H:%M:%S",      # 17/07/2025 08:00:00
        "%d/%m/%Y %H:%M",         # 17/07/2025 08:00
        "%d/%m/%Y",               # 17/07/2025
        # Formatos ISO
        "%Y-%m-%d %H:%M:%S",      # 2025-07-17 08:00:00
        "%Y-%m-%d %H:%M",         # 2025-07-17 08:00
        "%Y-%m-%d",               # 2025-07-17
        # Formatos alternativos com hora
        "%d/%m/%Y %H:%M:%S.%f",   # 17/07/2025 08:00:00.123456 (com microsegundos)
        "%Y-%m-%d %H:%M:%S.%f",   # 2025-07-17 08:00:00.123456 (com microsegundos)
    ]
    
    @staticmethod
    def _normalizar_cabecalho(texto: str) -> str:
        """Normaliza texto para comparação: remove acentos, lowercase, strip"""
        if not texto:
            return ""
        texto = str(texto).strip().lower()
        # Remove acentos (NFD decompõe, depois remove combining chars)
        texto = unicodedata.normalize('NFD', texto)
        texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
        # Remove caracteres especiais para match flexível
        texto = ''.join(c if c.isalnum() or c in ' _' else '' for c in texto)
        texto = ' '.join(texto.split())  # Normalizar espaços
        return texto

    @classmethod
    def _mapear_cabecalhos_flexivel(cls, headers: List[str]) -> Dict[str, str]:
        """
        Mapeia cabeçalhos do arquivo para os nomes esperados.
        Retorna dict: {header_original: nome_esperado}
        """
        mapeamento = {}
        headers_norm = {cls._normalizar_cabecalho(h): h for h in headers if h}
        
        for esperado, variantes in _CAMPO_CSV_ESPERADOS:
            encontrado = None
            esperado_norm = cls._normalizar_cabecalho(esperado)
            if esperado_norm in headers_norm:
                encontrado = headers_norm[esperado_norm]
            else:
                for var in variantes:
                    var_norm = cls._normalizar_cabecalho(var)
                    if var_norm in headers_norm:
                        encontrado = headers_norm[var_norm]
                        break
                    # Match parcial (contém)
                    for h_norm, h_orig in headers_norm.items():
                        if var_norm in h_norm or h_norm in var_norm:
                            encontrado = h_orig
                            break
                    if encontrado:
                        break
            if encontrado:
                mapeamento[encontrado] = esperado
        
        return mapeamento

    @classmethod
    def tem_estrutura_portabilidade(cls, headers: List[str]) -> bool:
        """
        Verifica se o arquivo tem estrutura de atualização portabilidade.
        Requer pelo menos: CPF, número de acesso e código externo (ou número da ordem).
        """
        mapeamento = cls._mapear_cabecalhos_flexivel(headers)
        valores_mapeados = [v.lower() for v in mapeamento.values()]
        tem_cpf = any('cpf' in v or 'documento' in v for v in valores_mapeados)
        tem_acesso = any('acesso' in v for v in valores_mapeados)
        tem_ordem_ou_codigo = any(
            'ordem' in v or 'codigo' in v or 'externo' in v for v in valores_mapeados
        )
        return tem_cpf and (tem_acesso or tem_ordem_ou_codigo)

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
    def _aplicar_mapeamento_row(cls, row: dict, mapeamento: Dict[str, str]) -> dict:
        """Aplica mapeamento de colunas à linha, retornando row com nomes esperados."""
        row_normalizado = {}
        for col_orig, col_esperado in mapeamento.items():
            if col_orig in row and row[col_orig] is not None:
                row_normalizado[col_esperado] = row[col_orig]
        return row_normalizado

    @classmethod
    def parse_file(cls, file_path: str) -> List[PortabilidadeRecord]:
        """
        Parse de arquivo CSV ou Excel completo.
        Redireciona para o parser apropriado conforme extensão.
        """
        path = Path(file_path)
        if path.suffix.lower() in ('.xlsx', '.xls'):
            return cls.parse_excel_file(file_path)
        return cls.parse_file_flexible(file_path)

    @classmethod
    def parse_excel_file(cls, file_path: str) -> List[PortabilidadeRecord]:
        """
        Parse de arquivo Excel com estrutura Siebel (portabilidade).
        Aceita mesma estrutura do CSV: Cpf, Número de acesso, Código externo, etc.
        Suporta .xlsx (openpyxl) e .xls legado (xlrd).
        """
        import pandas as pd

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

        # Escolher engine conforme extensão
        ext = path.suffix.lower()
        if ext == '.xls':
            try:
                df = pd.read_excel(file_path, engine='xlrd', dtype=str)
            except Exception:
                # Fallback: alguns .xls são na verdade HTML/XML renomeados
                try:
                    df = pd.read_html(file_path)[0].astype(str)
                except Exception as e2:
                    raise ValueError(
                        f"Não foi possível ler o arquivo .xls '{path.name}'. "
                        f"Tente salvar como .xlsx no Excel. Erro: {e2}"
                    )
        else:
            df = pd.read_excel(file_path, engine='openpyxl', dtype=str)

        headers = [str(c) for c in df.columns]
        mapeamento = cls._mapear_cabecalhos_flexivel(headers)

        records = []
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            row_norm = cls._aplicar_mapeamento_row(row_dict, mapeamento) if mapeamento else row_dict
            # Converter valores para string (Excel retorna float/nan para células vazias)
            row_norm = {
                k: ('' if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v).strip())
                for k, v in row_norm.items()
            }
            record = cls._parse_row(row_norm)
            if record:
                records.append(record)

        logger.info(f"Parseados {len(records)} registros do Excel {path.name}")
        return records

    @classmethod
    def parse_file_flexible(cls, file_path: str) -> List[PortabilidadeRecord]:
        """
        Parse de arquivo CSV com mapeamento flexível de cabeçalhos.
        Aceita variações de título (acentos, maiúsculas, sinônimos).
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Lista de registros de portabilidade
        """
        records = []
        
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        encoding_usado = None
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    file_content = f.read()
                    encoding_usado = encoding
                    break
            except (UnicodeDecodeError, LookupError):
                continue
        
        if file_content is None:
            raise ValueError(
                f"Erro ao ler arquivo {file_path}: nenhum encoding funcionou."
            )
        
        import io
        f = io.StringIO(file_content)
        # Detectar delimitador (virgula ou ponto-e-virgula comum em PT-BR)
        try:
            sample = file_content[:4096]
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
        except csv.Error:
            dialect = csv.excel  # fallback: virgula
        f2 = io.StringIO(file_content)
        reader = csv.DictReader(f2, dialect=dialect)
        headers = reader.fieldnames or []
        
        mapeamento = cls._mapear_cabecalhos_flexivel(headers)
        if mapeamento:
            logger.debug(f"Headers mapeados: {list(mapeamento.values())[:5]}...")
        
        for row_num, row in enumerate(reader, start=2):
            try:
                row_norm = cls._aplicar_mapeamento_row(row, mapeamento) if mapeamento else row
                record = cls._parse_row(row_norm)
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
